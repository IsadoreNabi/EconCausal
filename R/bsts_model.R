#' Bayesian Structural Time Series Model
#'
#' Implements Bayesian Structural Time Series models with Leave-Future-Out validation
#' for assessing causal relationships between economic variables with temporal stability.
#'
#' @param data_path Path to Excel file containing the data
#' @param circ_vars Character vector of circulation variable names
#' @param prod_vars Character vector of production variable names
#' @param max_lag Maximum number of lags for independent variables (default: 6)
#' @param lfo_init_frac Initial fraction for LFO (default: 0.8)
#' @param lfo_h Horizon for LFO (default: 6)
#' @param lfo_step Step size for LFO (default: 6)
#' @param niter Number of MCMC iterations (default: 2000)
#' @param burn Number of burn-in iterations (default: 500)
#' @param seed Random seed (default: 123)
#' @param seasonality Seasonality parameter (NULL for none, 12 for monthly)
#' @param support_min Minimum support threshold (default: 0.6)
#' @param folds_min Minimum number of folds required (default: 5)
#' @param sup_hi High support threshold (default: 0.7)
#' @param sup_lo Low support threshold (default: 0.6)
#' @param out_dir Output directory for results (default: "output_bsts")
#'
#' @return A list containing:
#' \item{rank_ss_all}{Full results for all pairs}
#' \item{winners_ss_070}{Pairs with support ≥ 0.70}
#' \item{winners_ss_060}{Pairs with support ≥ 0.60}
#' \item{summaries_ss}{Summary statistics}
#'
#' @details
#' This function implements Bayesian Structural Time Series models for assessing
#' causal relationships between economic variables. It uses Leave-Future-Out
#' cross-validation with tuning between Local Level and Local Linear Trend
#' specifications. The methodology is described in detail in the methodological
#' document "DETALLES METODOLÓGICOS SPACESTATE MODEL.docx".
#'
#' @examples
#' \dontrun{
#' # Example usage
#' result <- bsts_model(
#'   data_path = "path/to/data.xlsx",
#'   circ_vars = c("TC_SPOT_CAN_US", "TC_SPOT_US_CAN", "TC_SPOT_US_REMB",
#'                 "IPC", "TdI_LdelT", "TasaDescuento"),
#'   prod_vars = c("ValorExportaciones", "Real_Net_Profit", 
#'                 "RealSocialConsumptionPerWorker2017", "RealWage_PPP2017",
#'                 "CapitalStock_PPP2017", "LaborProductivity_PPP2017", 
#'                 "InvestmentPerWorker_PPP2017")
#' )
#' }
#'
#' @export
bsts_model <- function(data_path, circ_vars, prod_vars, max_lag = 6, lfo_init_frac = 0.8,
                       lfo_h = 6, lfo_step = 6, niter = 2000, burn = 500, seed = 123,
                       seasonality = NULL, support_min = 0.6, folds_min = 5, sup_hi = 0.7,
                       sup_lo = 0.6, out_dir = "output_bsts") {
  
  # Load required packages
  suppressPackageStartupMessages({
    library(bsts)
    library(dplyr)
    library(tidyr)
    library(purrr)
    library(plotly)
    library(readxl)
    library(lubridate)
    library(tibble)
  })
  
  # Create output directory
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  options(scipen = 0)
  
  # Utility functions
  simple_name <- function(nm) {
    nm <- gsub("^as\\.numeric\\.", "", nm)
    nm <- gsub("\\.NEW\\.$", "", nm)
    nm <- gsub("\\.1\\.588\\.$", "", nm)
    nm <- gsub("\\.+$", "", nm)
    nm <- gsub("\\.", "_", nm)
    nm
  }
  
  ensure_time_index <- function(df) {
    if (!"Month" %in% names(df)) {
      tc <- names(Filter(function(x) inherits(x, c("POSIXct", "POSIXt", "Date")), df))
      if (length(tc) == 0) stop("No temporal column found")
      names(df)[match(tc[1], names(df))] <- "Month"
    }
    df <- df %>% arrange(Month)
    if (!"time_idx" %in% names(df)) df <- df %>% mutate(time_idx = dplyr::row_number())
    df
  }
  
  make_lags <- function(x, max_lag) {
    as_tibble(setNames(lapply(1:max_lag, function(k) dplyr::lag(x, k)),
                       paste0("L", 1:max_lag)))
  }
  
  make_lfo_splits <- function(n, initial, h, step) {
    splits <- list()
    end_train <- initial
    while ((end_train + 1) <= n - 1) {
      st <- end_train + 1L
      ed <- min(end_train + h, n)
      if (st > ed) break
      splits[[length(splits) + 1L]] <- list(train = 1:end_train, test = st:ed)
      end_train <- min(end_train + step, n - 1L)
    }
    splits
  }
  
  rmse <- function(yhat, y) sqrt(mean((yhat - y)^2))
  mae  <- function(yhat, y) mean(abs(yhat - y))
  sd_from_q <- function(q025, q975) {
    pmax(q975 - q025, 1e-8) / (2 * qnorm(0.975))
  }
  
  elpd_gaussian <- function(y, mean_vec, sd_vec) {
    sdv <- pmax(sd_vec, 1e-8)
    sum(dnorm(y, mean = mean_vec, sd = sdv, log = TRUE))
  }
  
  coverage_and_pit_norm <- function(y, mean_vec, sd_vec) {
    sdv <- pmax(sd_vec, 1e-8)
    pit <- pnorm(y, mean = mean_vec, sd = sdv)
    cover80 <- mean(y >= (mean_vec + qnorm(.10) * sdv) & y <= (mean_vec + qnorm(.90) * sdv))
    cover95 <- mean(y >= (mean_vec + qnorm(.025) * sdv) & y <= (mean_vec + qnorm(.975) * sdv))
    list(cover80 = cover80, cover95 = cover95, pit = pit)
  }
  
  predict_stats_bsts <- function(model, h, newdata = NULL, burn = burn,
                                 probs80 = c(.10, .90), probs95 = c(.025, .975)) {
    pr <- predict(model, horizon = h, newdata = newdata, burn = burn,
                  quantiles = c(probs95[1], probs95[2]))
    
    # Try to get draws from prediction matrix
    draws <- NULL
    if (!is.null(pr$prediction.matrix) && is.matrix(pr$prediction.matrix)) {
      draws <- pr$prediction.matrix
    } else if (!is.null(pr$distribution)) {
      D <- pr$distribution
      if (is.matrix(D)) draws <- D
      else if (is.list(D) && length(D) > 0 && is.numeric(D[[1]])) draws <- do.call(rbind, D)
    }
    
    if (!is.null(draws) && is.matrix(draws) && nrow(draws) > 1) {
      mean_vec <- colMeans(draws)
      qu <- apply(draws, 2, quantile, probs = c(probs80, probs95), na.rm = TRUE)
      sdv <- apply(draws, 2, sd)
      sdv <- pmax(sdv, 1e-8)
      return(list(mean = mean_vec, sd = sdv, q10 = qu[1, ], q90 = qu[2, ], q025 = qu[3, ], q975 = qu[4, ]))
    }
    
    # Fallback to interval
    if (!is.null(pr$interval) && is.matrix(pr$interval) && ncol(pr$interval) >= 2) {
      mean_vec <- as.numeric(pr$mean)
      q025 <- pr$interval[, 1]
      q975 <- pr$interval[, ncol(pr$interval)]
      sdv  <- sd_from_q(q025, q975)
      q10  <- mean_vec + qnorm(probs80[1]) * sdv
      q90  <- mean_vec + qnorm(probs80[2]) * sdv
      sdv  <- pmax(sdv, 1e-8)
      return(list(mean = mean_vec, sd = sdv, q10 = q10, q90 = q90, q025 = q025, q975 = q975))
    }
    
    stop("predict() did not return draws or interval")
  }
  
  build_state_spec <- function(y, trend = FALSE, season = NULL) {
    ss <- AddLocalLevel(list(), y = y)
    if (isTRUE(trend)) ss <- AddLocalLinearTrend(ss, y = y)
    if (!is.null(season)) ss <- AddSeasonal(ss, y = y, nseasons = season)
    ss
  }
  
  make_reg_prior <- function(Xmat, y) {
    if (is.null(Xmat) || ncol(Xmat) == 0) return(NULL)
    SpikeSlabPrior(x = Xmat, y = y,
                   expected.model.size = max(1, min(5, ncol(Xmat))),
                   prior.information.weight = 0.01,
                   diagonal.shrinkage = 0.5)
  }
  
  # Load and prepare data
  DATA <- read_excel(data_path) %>%
    rename_with(simple_name) %>%
    ensure_time_index()
  
  # Check if we have the expected number of variables
  if (length(circ_vars) != 6L || length(prod_vars) != 7L) {
    stop("Incorrect number of circulation or production variables")
  }
  
  # Create all pairs (both directions)
  pairs_ss <- dplyr::bind_rows(
    tidyr::expand_grid(Y = prod_vars, X = circ_vars),
    tidyr::expand_grid(Y = circ_vars, X = prod_vars)
  )
  
  # Main processing loop
  out_list_ss <- vector("list", nrow(pairs_ss))
  
  for (i in seq_len(nrow(pairs_ss))) {
    cat(sprintf("[BSTS %d/%d] %s <- %s\n", i, nrow(pairs_ss), pairs_ss$Y[i], pairs_ss$X[i]))
    
    # Fit pair with LFO and tuning
    result <- tryCatch(
      fit_pair_bsts_lfo_tuned(DATA, Y = pairs_ss$Y[i], X = pairs_ss$X[i]),
      error = function(e) {
        cat(sprintf("Error processing pair %s -> %s: %s\n", pairs_ss$X[i], pairs_ss$Y[i], e$message))
        NULL
      }
    )
    
    out_list_ss[[i]] <- result
  }
  
  # Process results
  summaries_ss <- purrr::map_dfr(out_list_ss, function(x) {
    if (is.null(x) || is.null(x$best_summary)) return(tibble())
    x$best_summary
  })
  
  if (nrow(summaries_ss) == 0) {
    warning("No valid results obtained from any pair")
    return(NULL)
  }
  
  # Ranking and filtering
  rank_ss_all <- summaries_ss %>%
    mutate(pair = paste0(X, " -> ", Y)) %>%
    arrange(desc(support), desc(dELPD_mean), desc(dRMSE_mean)) %>%
    mutate(pass_support = (support >= support_min) & (folds >= folds_min))
  
  winners_ss_070 <- rank_ss_all %>%
    filter(pass_support, support >= sup_hi, dELPD_mean > 0, dRMSE_mean > 0) %>%
    arrange(desc(support), desc(dELPD_mean), desc(dRMSE_mean))
  
  winners_ss_060 <- rank_ss_all %>%
    filter(pass_support, support >= sup_lo, dELPD_mean > 0, dRMSE_mean > 0) %>%
    arrange(desc(support), desc(dELPD_mean), desc(dRMSE_mean))
  
  # Export results
  f_all <- file.path(out_dir, "bsts_rank_all_pairs.csv")
  f_hi  <- file.path(out_dir, "bsts_winners_sup70.csv")
  f_lo  <- file.path(out_dir, "bsts_winners_sup60.csv")
  
  write.csv(rank_ss_all, f_all, row.names = FALSE)
  write.csv(winners_ss_070, f_hi, row.names = FALSE)
  write.csv(winners_ss_060, f_lo, row.names = FALSE)
  
  # Return results
  return(list(
    rank_ss_all = rank_ss_all,
    winners_ss_070 = winners_ss_070,
    winners_ss_060 = winners_ss_060,
    summaries_ss = summaries_ss
  ))
}