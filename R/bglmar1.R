#' Bayesian Generalized Linear Model with AR(1) Errors
#'
#' Implements a Bayesian GLM with autoregressive errors of order 1 for causal inference
#' between economic variables, with emphasis on temporal stability through Leave-Future-Out
#' cross-validation.
#'
#' @param data_path Path to Excel file containing the data
#' @param circ_vars Character vector of circulation variable names
#' @param prod_vars Character vector of production variable names
#' @param max_lag Maximum number of lags for independent variables (default: 3)
#' @param initial_frac Initial fraction of data for training (default: 0.7)
#' @param initial_min Minimum number of observations for initial training (default: 90)
#' @param test_h Test horizon in months (default: 12)
#' @param step_h Step size between folds in months (default: 12)
#' @param lfo_window Type of window for LFO ("sliding" or "expanding", default: "sliding")
#' @param chains Number of MCMC chains (default: 4)
#' @param parallel_chains Number of parallel chains (default: 4)
#' @param iter Total iterations per chain (default: 1500)
#' @param warmup Warmup iterations per chain (default: 750)
#' @param adapt_delta Adapt delta parameter for NUTS (default: 0.95)
#' @param trees Maximum tree depth for NUTS (default: 12)
#' @param seed Random seed (default: 2025)
#' @param support_min Minimum support threshold for stable relationships (default: 0.6)
#' @param folds_min Minimum number of folds required (default: 5)
#' @param sup_hi High support threshold (default: 0.7)
#' @param sup_lo Low support threshold (default: 0.6)
#' @param backend Backend for Stan compilation: "auto" (default), "rstan", or "cmdstanr".
#'   If "auto", the function uses 'rstan' when available, otherwise tries 'cmdstanr'.
#'
#' @return A list containing:
#' \item{bench_bayes}{Full results for all pairs}
#' \item{winners_070}{Pairs with support >= 0.70}
#' \item{winners_060}{Pairs with support >= 0.60}
#' \item{rank_out}{Output from ranking function}
#'
#' @details
#' This function implements a Bayesian GLM with AR(1) errors for assessing causal relationships
#' between economic variables. It uses Leave-Future-Out cross-validation with sliding windows
#' to evaluate temporal stability of relationships. The function no longer requires 'cmdstanr'
#' at install time; if 'backend = "cmdstanr"' is requested but 'cmdstanr' (and a working CmdStan)
#' are not available, it gracefully falls back to 'rstan'. In any case, heavy computations are
#' not run in package examples or tests.
#'
#' @examples
#' \dontrun{
#' # Example usage
#' result <- bglmar1(
#'   data_path = "path/to/data.xlsx",
#'   circ_vars = c("TC_SPOT_CAN_US", "TC_SPOT_US_CAN", "TC_SPOT_US_REMB",
#'                 "IPC", "TdI_LdelT", "TasaDescuento"),
#'   prod_vars = c("ValorExportaciones", "Real_Net_Profit", 
#'                 "RealSocialConsumptionPerWorker2017", "RealWage_PPP2017",
#'                 "CapitalStock_PPP2017", "LaborProductivity_PPP2017", 
#'                 "InvestmentPerWorker_PPP2017"),
#'   backend = "auto"
#' )
#' }
#'
#' @export
bglmar1 <- function(data_path, circ_vars, prod_vars, max_lag = 3, initial_frac = 0.7, 
                    initial_min = 90, test_h = 12, step_h = 12, lfo_window = "sliding",
                    chains = 4, parallel_chains = 4, iter = 1500, warmup = 750,
                    adapt_delta = 0.95, trees = 12, seed = 2025, support_min = 0.6,
                    folds_min = 5, sup_hi = 0.7, sup_lo = 0.6, backend = c("auto","rstan","cmdstanr")) {
  
  backend <- match.arg(backend)
  pick_backend <- function(pref = "auto") {
    # user preference via option takes precedence
    opt <- getOption("EconCausal.backend", NA_character_)
    if (!is.na(opt)) pref <- opt
    
    if (identical(pref, "cmdstanr")) {
      if (requireNamespace("cmdstanr", quietly = TRUE)) {
        ver <- try(cmdstanr::cmdstan_version(error_on_NA = FALSE), silent = TRUE)
        if (!inherits(ver, "try-error") && !is.null(ver)) return("cmdstanr")
      }
      message("EconCausal: 'cmdstanr' not available; trying 'rstan'.")
      pref <- "rstan"
    }
    if (identical(pref, "rstan") || identical(pref, "auto")) {
      if (requireNamespace("rstan", quietly = TRUE)) return("rstan")
    }
    if (!identical(pref, "cmdstanr")) {
      if (requireNamespace("cmdstanr", quietly = TRUE)) {
        ver <- try(cmdstanr::cmdstan_version(error_on_NA = FALSE), silent = TRUE)
        if (!inherits(ver, "try-error") && !is.null(ver)) return("cmdstanr")
      }
    }
    stop("Neither 'rstan' nor 'cmdstanr' is available. Please install one of them to fit models.")
  }
  backend_used <- pick_backend(backend)
  
  # Set execution preferences (no effect during CRAN checks; examples are \\dontrun)
  options(mc.cores = parallel::detectCores())
  options(scipen = 0)
  
  # Load data
  if (!exists("DATA")) {
    DATA <- readxl::read_excel(data_path)
  }
  
  # Clean variable names
  simple_name <- function(nm) {
    nm <- gsub("^as\\.numeric\\.", "", nm)
    nm <- gsub("\\.NEW\\.$", "", nm)
    nm <- gsub("\\.1\\.588\\.$", "", nm)
    nm <- gsub("\\.+$", "", nm)
    nm <- gsub("\\.", "_", nm)
    nm
  }
  
  DATA <- DATA %>% dplyr::rename_with(simple_name)
  
  # Ensure Month column exists and create time index
  if (!"Month" %in% names(DATA)) {
    time_candidates <- names(Filter(function(x) inherits(x, c("POSIXct","POSIXt","Date")), DATA))
    if (length(time_candidates) == 0) stop("No temporal column found")
    names(DATA)[match(time_candidates[1], names(DATA))] <- "Month"
  }
  
  DATA <- DATA %>% dplyr::arrange(.data$Month) %>% dplyr::mutate(time_idx = dplyr::row_number())
  
  # Check if we have the expected number of variables
  present <- setdiff(names(DATA), "Month")
  
  if (length(circ_vars) != 6L || length(prod_vars) != 7L) {
    stop("Incorrect number of circulation or production variables")
  }
  
  # Create all pairs (both directions)
  pairs <- rbind(
    expand.grid(Y = prod_vars, X = circ_vars, stringsAsFactors = FALSE),
    expand.grid(Y = circ_vars, X = prod_vars, stringsAsFactors = FALSE)
  )
  
  # Utility functions
  make_lags <- function(x, L) {
    out <- as.data.frame(sapply(1:L, function(k) dplyr::lag(x, k)))
    names(out) <- paste0("X_l", 1:L)
    out
  }
  
  make_splits <- function(n, initial, test, step, window = c("expanding","sliding")){
    window <- match.arg(window)
    splits <- list()
    start_train <- 1L
    end_train   <- initial
    while ((end_train + 1) <= n - 1) {
      start_test <- end_train + 1L
      end_test   <- min(end_train + test, n)
      if (start_test > end_test) break
      if (window == "expanding") {
        tr <- start_train:end_train
      } else {
        sz  <- end_train - start_train + 1L
        win <- min(sz, initial)
        tr  <- (end_train - win + 1L):end_train
      }
      te <- start_test:end_test
      splits[[length(splits)+1L]] <- list(train=tr, test=te)
      end_train <- min(end_train + step, n - 1L)
    }
    splits
  }
  
  smape <- function(pred, obs) {
    ok <- is.finite(pred) & is.finite(obs) & (abs(pred)+abs(obs) > .Machine$double.eps)
    if (!any(ok)) return(NA_real_)
    mean( 2*abs(pred[ok]-obs[ok]) / (abs(pred[ok]) + abs(obs[ok])) ) * 100
  }
  
  safe_r2 <- function(pred, obs) {
    SST <- sum((obs - mean(obs))^2)
    if (!is.finite(SST) || SST <= .Machine$double.eps) return(NA_real_)
    1 - sum((pred-obs)^2)/SST
  }
  
  log_mean_exp <- function(v) { m <- max(v); m + log(mean(exp(v - m))) }
  
  # Prepare base frame with temporal index
  df <- DATA %>%
    dplyr::arrange(.data$Month) %>%
    dplyr::mutate(t_index = seq_len(n()))
  
  # Priors for standardized scale (Y_s ~ N(0,1))
  pri <- c(
    brms::set_prior("normal(0, 1)",         class = "b"),
    brms::set_prior("student_t(3, 0, 2.5)", class = "Intercept"),
    brms::set_prior("exponential(1)",       class = "sigma")
  )
  
  # Main loop over pairs with LFO
  res_list <- vector("list", nrow(pairs))
  
  for (pp in seq_len(nrow(pairs))) {
    Ynm <- pairs$Y[pp]
    Xnm <- pairs$X[pp]
    
    # Frame for the pair + lags of X
    dat0 <- df %>%
      dplyr::select(.data$Month, .data$t_index, dplyr::all_of(c(Ynm, Xnm))) %>%
      dplyr::rename(Y = !!Ynm, X = !!Xnm) %>%
      dplyr::mutate(dplyr::across(c(.data$Y, .data$X), as.numeric))
    
    dat <- dat0 %>%
      dplyr::bind_cols(make_lags(dat0$X, max_lag)) %>%
      tidyr::drop_na()
    
    n <- nrow(dat)
    # Need space after lags for first window + test
    if (n < (initial_min + test_h + max_lag + 5)) {
      res_list[[pp]] <- tibble::tibble(
        pair = paste0(Xnm, " -> ", Ynm),
        folds = 0, folds_pass = 0, support = NA_real_,
        ELPD_diff_mean = NA_real_, RMSE_diff_mean = NA_real_,
        RMSE_full_mean = NA_real_, RMSE_base_mean = NA_real_,
        MAE_full_mean  = NA_real_, MAE_base_mean  = NA_real_,
        sMAPE_full_mean= NA_real_, sMAPE_base_mean= NA_real_,
        R2_full_mean   = NA_real_, R2_base_mean   = NA_real_
      )
      next
    }
    
    initial <- max(initial_min, floor(initial_frac * n))
    splits  <- make_splits(n, initial, test_h, step_h, window = lfo_window)
    
    if (length(splits) == 0L) {
      res_list[[pp]] <- tibble::tibble(
        pair = paste0(Xnm, " -> ", Ynm),
        folds = 0, folds_pass = 0, support = NA_real_,
        ELPD_diff_mean = NA_real_, RMSE_diff_mean = NA_real_,
        RMSE_full_mean = NA_real_, RMSE_base_mean = NA_real_,
        MAE_full_mean  = NA_real_, MAE_base_mean  = NA_real_,
        sMAPE_full_mean= NA_real_, sMAPE_base_mean= NA_real_,
        R2_full_mean   = NA_real_, R2_base_mean   = NA_real_
      )
      next
    }
    
    # Containers per fold
    wins <- logical(0)
    elpd_diffs <- numeric(0)
    rmse_diffs <- numeric(0)
    
    rmse_full_v <- mae_full_v <- smape_full_v <- r2_full_v <- numeric(0)
    rmse_base_v <- mae_base_v <- smape_base_v <- r2_base_v <- numeric(0)
    
    for (ff in seq_along(splits)) {
      sp <- splits[[ff]]
      train <- dat[sp$train, , drop = FALSE]
      test  <- dat[sp$test,  , drop = FALSE]
      
      # Standardization by fold (critical for NUTS)
      mu_y <- mean(train$Y); sd_y <- stats::sd(train$Y)
      if (!is.finite(sd_y) || sd_y <= .Machine$double.eps) {
        next
      }
      train$Y_s <- (train$Y - mu_y) / sd_y
      test$Y_s  <- (test$Y  - mu_y) / sd_y
      
      # Scale time (centered near 0, reasonable range)
      t_mu <- mean(train$t_index); t_sd <- stats::sd(train$t_index)
      if (!is.finite(t_sd) || t_sd <= .Machine$double.eps) t_sd <- 1
      train$t_s <- (train$t_index - t_mu) / t_sd
      test$t_s  <- (test$t_index  - t_mu) / t_sd
      
      # Scale X lags with train statistics
      x_lag_names <- paste0("X_l", 1:max_lag)
      for (nm in x_lag_names) {
        mu_x <- mean(train[[nm]], na.rm = TRUE)
        sd_x <- stats::sd(train[[nm]], na.rm = TRUE)
        if (!is.finite(sd_x) || sd_x <= .Machine$double.eps) {
          train[[nm]] <- NULL; test[[nm]] <- NULL
        } else {
          train[[nm]] <- (train[[nm]] - mu_x) / sd_x
          test[[nm]]  <- (test[[nm]]  - mu_x) / sd_x
        }
      }
      x_lag_used <- intersect(x_lag_names, names(train))
      
      # Need a "series" for AR(1)
      train$series <- factor("one")
      test$series  <- factor("one")
      
      # brms formulas (AR1)
      f_base <- brms::bf(
        Y_s ~ 1 + t_s,
        autocor = brms::cor_ar(~ t_index | series, p = 1)
      )
      rhs <- paste(c("1", "t_s", x_lag_used), collapse = " + ")
      f_full <- brms::bf(
        stats::as.formula(paste("Y_s ~", rhs)),
        autocor = brms::cor_ar(~ t_index | series, p = 1)
      )
      
      # Model fitting with selected backend
      m_base <- tryCatch(
        brms::brm(
          formula = f_base, data = train, family = stats::gaussian(), prior = pri,
          chains = chains, iter = iter, warmup = warmup, seed = seed + 101,
          backend = backend_used, refresh = 50,
          cores = parallel_chains,
          control = list(adapt_delta = adapt_delta, max_treedepth = trees)
        ),
        error = function(e) NULL
      )
      m_full <- tryCatch(
        brms::brm(
          formula = f_full, data = train, family = stats::gaussian(), prior = pri,
          chains = chains, iter = iter, warmup = warmup, seed = seed + 202,
          backend = backend_used, refresh = 50,
          cores = parallel_chains,
          control = list(adapt_delta = adapt_delta, max_treedepth = trees)
        ),
        error = function(e) NULL
      )
      if (is.null(m_base) || is.null(m_full)) next
      
      # ELPD fold (NEW DATA)
      ll_base <- tryCatch(brms::log_lik(m_base, newdata = test, re_formula = NA), error = function(e) NULL)
      ll_full <- tryCatch(brms::log_lik(m_full, newdata = test, re_formula = NA), error = function(e) NULL)
      if (is.null(ll_base) || is.null(ll_full)) {
        next
      }
      elpd_base <- sum(apply(ll_base, 2, log_mean_exp))
      elpd_full <- sum(apply(ll_full, 2, log_mean_exp))
      elpd_diff <- elpd_full - elpd_base
      
      # OOS predictions (posterior mean) and metrics in original scale
      ep_base <- brms::posterior_epred(m_base, newdata = test, re_formula = NA)
      ep_full <- brms::posterior_epred(m_full, newdata = test, re_formula = NA)
      yhat_base <- colMeans(ep_base) * sd_y + mu_y
      yhat_full <- colMeans(ep_full) * sd_y + mu_y
      
      obs <- test$Y
      rmse_base <- sqrt(mean((yhat_base - obs)^2))
      rmse_full <- sqrt(mean((yhat_full - obs)^2))
      mae_base  <- mean(abs(yhat_base - obs))
      mae_full  <- mean(abs(yhat_full - obs))
      sm_base   <- smape(yhat_base, obs)
      sm_full   <- smape(yhat_full, obs)
      r2_base   <- safe_r2(yhat_base, obs)
      r2_full   <- safe_r2(yhat_full, obs)
      
      # WIN if improves ELPD and lowers RMSE
      win <- is.finite(elpd_diff) && (elpd_diff > 0) &&
        is.finite(rmse_base) && is.finite(rmse_full) && (rmse_full < rmse_base)
      
      wins        <- c(wins, win)
      elpd_diffs  <- c(elpd_diffs, elpd_diff)
      rmse_diffs  <- c(rmse_diffs, rmse_full - rmse_base)
      
      rmse_full_v <- c(rmse_full_v, rmse_full)
      rmse_base_v <- c(rmse_base_v, rmse_base)
      mae_full_v  <- c(mae_full_v,  mae_full)
      mae_base_v  <- c(mae_base_v,  mae_base)
      smape_full_v<- c(smape_full_v,sm_full)
      smape_base_v<- c(smape_base_v,sm_base)
      r2_full_v   <- c(r2_full_v,   r2_full)
      r2_base_v   <- c(r2_base_v,   r2_base)
    } # end folds
    
    folds <- length(wins)
    if (folds == 0L) {
      res_list[[pp]] <- tibble::tibble(
        pair = paste0(Xnm, " -> ", Ynm),
        folds = 0, folds_pass = 0, support = NA_real_,
        ELPD_diff_mean = NA_real_, RMSE_diff_mean = NA_real_,
        RMSE_full_mean = NA_real_, RMSE_base_mean = NA_real_,
        MAE_full_mean  = NA_real_, MAE_base_mean  = NA_real_,
        sMAPE_full_mean= NA_real_, sMAPE_base_mean= NA_real_,
        R2_full_mean   = NA_real_, R2_base_mean   = NA_real_
      )
    } else {
      folds_pass <- sum(wins, na.rm = TRUE)
      support    <- folds_pass / folds
      res_list[[pp]] <- tibble::tibble(
        pair = paste0(Xnm, " -> ", Ynm),
        folds = folds, folds_pass = folds_pass, support = support,
        ELPD_diff_mean = if (length(elpd_diffs)) mean(elpd_diffs, na.rm = TRUE) else NA_real_,
        RMSE_diff_mean = if (length(rmse_diffs)) mean(rmse_diffs, na.rm = TRUE) else NA_real_,
        RMSE_full_mean = if (length(rmse_full_v)) mean(rmse_full_v, na.rm = TRUE) else NA_real_,
        RMSE_base_mean = if (length(rmse_base_v)) mean(rmse_base_v, na.rm = TRUE) else NA_real_,
        MAE_full_mean  = if (length(mae_full_v))  mean(mae_full_v,  na.rm = TRUE) else NA_real_,
        MAE_base_mean  = if (length(mae_base_v))  mean(mae_base_v,  na.rm = TRUE) else NA_real_,
        sMAPE_full_mean= if (length(smape_full_v))mean(smape_full_v,na.rm = TRUE) else NA_real_,
        sMAPE_base_mean= if (length(smape_base_v))mean(smape_base_v,na.rm = TRUE) else NA_real_,
        R2_full_mean   = if (length(r2_full_v))   mean(r2_full_v,   na.rm = TRUE) else NA_real_,
        R2_base_mean   = if (length(r2_base_v))   mean(r2_base_v,   na.rm = TRUE) else NA_real_
      )
    }
  }
  
  bench_bayes <- dplyr::bind_rows(res_list) %>%
    dplyr::arrange(dplyr::desc(.data$support), dplyr::desc(.data$ELPD_diff_mean), .data$RMSE_diff_mean)
  
  # Ranking function
  rank_bglm_results <- function(bench = bench_bayes,
                                sup_hi = sup_hi, sup_lo = sup_lo,
                                min_folds = folds_min,
                                out_all = "bglm_rank_all_pairs.csv",
                                out_hi  = "bglm_winners_sup70.csv",
                                out_lo  = "bglm_winners_sup60.csv") {
    
    top_all <- bench %>%
      dplyr::arrange(dplyr::desc(.data$support), dplyr::desc(.data$ELPD_diff_mean), .data$RMSE_diff_mean)
    
    winners_hi <- bench %>%
      dplyr::filter(.data$folds >= min_folds, is.finite(.data$support), .data$support >= sup_hi,
             .data$ELPD_diff_mean > 0, .data$RMSE_diff_mean < 0) %>%
      dplyr::arrange(dplyr::desc(.data$support), dplyr::desc(.data$ELPD_diff_mean), .data$RMSE_diff_mean)
    
    winners_lo <- bench %>%
      dplyr::filter(.data$folds >= min_folds, is.finite(.data$support), .data$support >= sup_lo,
             .data$ELPD_diff_mean > 0, .data$RMSE_diff_mean < 0) %>%
      dplyr::arrange(dplyr::desc(.data$support), dplyr::desc(.data$ELPD_diff_mean), .data$RMSE_diff_mean)
    
    # Useful ratios
    bench_ratios <- bench %>%
      dplyr::mutate(
        RMSE_ratio = .data$RMSE_full_mean / .data$RMSE_base_mean,
        MAE_ratio  = .data$MAE_full_mean  / .data$MAE_base_mean
      ) %>%
      dplyr::arrange(.data$RMSE_ratio) %>%
      dplyr::select(.data$pair, .data$support, .data$ELPD_diff_mean, .data$RMSE_diff_mean, .data$RMSE_ratio, .data$MAE_ratio)
    
    # Export
    utils::write.csv(top_all,    out_all, row.names = FALSE)
    utils::write.csv(winners_hi, out_hi,  row.names = FALSE)
    utils::write.csv(winners_lo, out_lo,  row.names = FALSE)
    
    invisible(list(all = top_all, winners_hi = winners_hi, winners_lo = winners_lo, ratios = bench_ratios))
  }
  
  rank_out <- rank_bglm_results()
  
  # Return results
  return(list(
    bench_bayes = bench_bayes,
    winners_070 = rank_out$winners_hi,
    winners_060 = rank_out$winners_lo,
    rank_out = rank_out
  ))
}
