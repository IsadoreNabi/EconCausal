test_that("ecm_mars() aborts when the workbook does not exist", {
  missing <- ec_missing_workbook()
  expect_false(file.exists(missing))
  expect_error(
    ecm_mars(data_path = missing,
             circ_vars = ec_circ_dotted,
             prod_vars = ec_prod_dotted,
             parallel_enable = FALSE)
  )
})

test_that("the workbook width check accepts exactly one column per name", {
  names14 <- paste0("v", 1:14)
  table_of <- function(n) as.data.frame(matrix(0, nrow = 2L, ncol = n))
  expect_invisible(EconCausal:::check_workbook_width(table_of(14L), names14))
  expect_error(EconCausal:::check_workbook_width(table_of(13L), names14),
               "must have exactly 14 columns", fixed = TRUE)
  expect_error(EconCausal:::check_workbook_width(table_of(15L), names14),
               "must have exactly 14 columns", fixed = TRUE)
})

test_that("ecm_mars() rejects a workbook of the wrong width before fitting", {
  # The shipped readxl workbook has eleven columns. Without the check the call
  # would run to the end and return a table of missing metrics.
  expect_error(
    ecm_mars(data_path = ec_foreign_workbook(),
             circ_vars = ec_circ_dotted,
             prod_vars = ec_prod_dotted,
             parallel_enable = FALSE),
    "must have exactly 14 columns",
    fixed = TRUE
  )
})

# The two tests below start from a state the function never installs by itself,
# so a missing on.exit() is visible: the function leaves a plain sequential plan
# and a single thread, and both differ from the marked starting state.

test_that("ecm_mars() restores the future plan after failing", {
  previous <- future::plan(future::sequential, split = TRUE)
  on.exit(future::plan(previous), add = TRUE)
  before <- future::plan("list")
  expect_error(
    ecm_mars(data_path = ec_missing_workbook(),
             circ_vars = ec_circ_dotted,
             prod_vars = ec_prod_dotted,
             parallel_enable = FALSE)
  )
  expect_equal(future::plan("list"), before)
})

test_that("ecm_mars() restores the BLAS and OpenMP thread counts after failing", {
  skip_if_not_installed("RhpcBLASctl")
  blas_previous <- RhpcBLASctl::blas_get_num_procs()
  omp_previous <- RhpcBLASctl::omp_get_max_threads()
  on.exit({
    RhpcBLASctl::blas_set_num_threads(blas_previous)
    RhpcBLASctl::omp_set_num_threads(omp_previous)
  }, add = TRUE)
  RhpcBLASctl::blas_set_num_threads(2)
  RhpcBLASctl::omp_set_num_threads(2)
  expect_error(
    ecm_mars(data_path = ec_missing_workbook(),
             circ_vars = ec_circ_dotted,
             prod_vars = ec_prod_dotted,
             parallel_enable = FALSE)
  )
  expect_identical(RhpcBLASctl::blas_get_num_procs(), 2L)
  expect_identical(RhpcBLASctl::omp_get_max_threads(), 2L)
})
