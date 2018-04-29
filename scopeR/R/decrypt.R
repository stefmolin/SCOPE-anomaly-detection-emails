#' @title Decrypt File
#' @description Decrypts a file that was encrypted using a common key.
#' @author Stefanie Molin
#'
#' @importFrom digest AES
#'
#' @param filename Path to the file to decrypt
#'
#' @return Dataframe with the results
#'
#' @export
decrypt <- function(filename) {
  dat <- readBin(filename, "raw", n = 1000)
  aes <- digest::AES(key, mode = "ECB")
  raw <- aes$decrypt(dat, raw = TRUE)
  txt <- rawToChar(raw[raw > 0])
  return(read.csv(text = txt, stringsAsFactors = FALSE))
}

#' @title Encrypt File
#' @description Encrypts a file using a common key.
#' @author Stefanie Molin
#'
#' @importFrom digest AES
#'
#' @param df Dataframe containing the login and password in that order.
#' @param filename Where to save the encrypted credentials
#'
#' @return Dataframe with the results
#'
#' @export
encrypt <- function(df, filename) {
  zz <- textConnection("out", "w")
  write.csv(df, zz, row.names = F)
  close(zz)
  out <- paste(out, collapse = "\n")
  raw <- charToRaw(out)
  raw <- c(raw,as.raw(rep(0, 16 - length(raw)%%16)))
  aes <- digest::AES(key, mode = "ECB")
  aes$encrypt(raw)
  writeBin(aes$encrypt(raw), filename)
}