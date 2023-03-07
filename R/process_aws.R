
#' Process TAHMO data.
#'
#' Read the AWS data from the local replicator of TAHMO, parse and convert into ADT format.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder.\cr
#' @param dirTAHMO full path to the directory TAHMO folder on ADT server.
#' 
#' @export

process.tahmo <- function(dirAWS, dirTAHMO){
    Sys.setenv(TZ = "Africa/Bamako")
    mon <- format(Sys.time(), '%Y%m')
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, paste0("processing_tahmo_", mon, ".txt"))

    ret <- try(get.tahmo.data(dirAWS, dirTAHMO), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Processing TAHMO data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}

#' Process PULSONIC data.
#'
#' Read the AWS data from the local replicator of PULSONIC, parse and convert into ADT format.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder.\cr
#' @param dirPLUSO full path to the directory PULSONIC folder on ADT server.
#' 
#' @export

process.pulsonic <- function(dirAWS, dirPLUSO){
    Sys.setenv(TZ = "Africa/Bamako")
    mon <- format(Sys.time(), '%Y%m')
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "PULSONIC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, paste0("processing_pulsonic_", mon, ".txt"))

    ret <- try(get.pulsonic.data(dirAWS, dirPLUSO), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Processing PULSONIC data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}


#' Process CAMPBELL SCIENTIFIC data.
#'
#' Read the AWS data from the local folder of CAMPBELL SCIENTIFIC, parse and convert into ADT format.
#' 
#' @param dirAWS full path to the directory containing the AWS_DATA folder.\cr
#' @param dirCBS full path to the directory CAMPBELL SCIENTIFIC folder on ADT server.
#' 
#' @export

process.campbellsci <- function(dirAWS, dirCBS){
    Sys.setenv(TZ = "Africa/Bamako")
    mon <- format(Sys.time(), '%Y%m')
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "CAMPBELLSCI")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, paste0("processing_campbellsci_", mon, ".txt"))

    ret <- try(get.campbellsci.data(dirAWS, dirCBS), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- "Processing CAMPBELL SCIENTIFIC data failed"
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}
