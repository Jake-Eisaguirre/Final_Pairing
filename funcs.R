Cols_AllMissing <- function(final_pairing){ # helper function
  as.vector(which(colSums(is.na(final_pairing)) == nrow(final_pairing)))
}