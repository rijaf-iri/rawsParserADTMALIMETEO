
get.tahmo.data <- function(dirAWS, dirTAHMO){
    tz <- "Africa/Bamako"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    awsNET <- 2

    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "minutes", "TAHMO")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_tahmo_", mon, ".txt"))
    awsLOG <- file.path(dirLOG, paste0("AWS_LOG_", mon, ".txt"))

    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                  stringsAsFactors = FALSE, quote = "\"",
                                  fileEncoding = "latin1")
    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_lastDates.csv")
    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"",
                                 fileEncoding = 'latin1')

    dirDATA <- file.path(dirTAHMO, "DATA")

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        tahmoID <- strsplit(awsID, '-')[[1]][1]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        pattern <- paste0('^', tahmoID, '_', '.+\\.csv$')
        aws_list <- list.files(dirDATA, pattern)

        if(length(aws_list) == 0) next

        awsL <- strsplit(aws_list, "_")
        len <- sapply(awsL, 'length')
        ilen <- len == 3
        if(!any(ilen)) next

        aws_list <- aws_list[ilen]
        awsL <- awsL[ilen]
        awsL <- do.call(rbind, awsL)
        awsL[, 3] <- gsub('\\.csv', '', awsL[, 3])

        start <- strptime(awsL[, 2], '%Y%m%d%H%M%S', tz)
        end <- strptime(awsL[, 3], '%Y%m%d%H%M%S', tz)

        ina <- is.na(start) | is.na(end)
        aws_list <- aws_list[!ina]
        if(length(aws_list) == 0) next

        start <- start[!ina]
        end <- end[!ina]

        time0 <- as.POSIXct(awsInfo$last[j], origin = origin, tz = tz)
        if(is.na(time0)) time0 <- start[order(start)][1]
        time1 <- Sys.time()

        it <- (end > time0) & (start <= time1)
        if(!any(it)) next
        aws_list <- aws_list[it]

        for(i in seq_along(aws_list)){
            aws_file <- file.path(dirDATA, aws_list[i])
            awsDAT <- utils::read.table(aws_file, sep = ',', header = TRUE,
                                        stringsAsFactors = FALSE, quote = "\"",
                                        fileEncoding = 'latin1')

            out <- try(parse.tahmo.data(awsDAT, awsVAR, awsID, awsNET), silent = TRUE)
            if(inherits(out, "try-error")){
                mserr <- gsub('[\r\n]', '', out[1])
                msg <- paste("Unable to parse data for", awsID, "File", aws_list[i])
                format.out.msg(paste(mserr, '\n', msg), awsLOG)
                next
            }

            if(is.null(out)) next

            awsInfo$last[j] <- max(out$obs_time)

            locFile <- paste(range(out$obs_time), collapse = "_")
            locFile <- paste0(awsID, "_", locFile, '.rds')
            locFile <- file.path(dirOUT, locFile)
            saveRDS(out, locFile)

            utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                               row.names = FALSE, quote = FALSE)
        }
    }

    return(0)
}

parse.tahmo.data <- function(awsDAT, awsVAR, awsID, awsNET){
    tz <- "Africa/Bamako"
    tahmo_time <- "%Y-%m-%dT%H:%M:%SZ"
    Sys.setenv(TZ = tz)

    temps <- strptime(awsDAT$time, tahmo_time, tz = "UTC")
    ina <- is.na(temps)
    awsDAT <- awsDAT[!ina, , drop = FALSE]
    if(nrow(awsDAT) == 0) return(NULL)

    temps <- temps[!ina]
    # temps <- time_utc2time_local(temps, tz)

    iok <- awsDAT$quality == 1
    awsDAT <- awsDAT[iok, , drop = FALSE]
    if(nrow(awsDAT) == 0) return(NULL)
    temps <- temps[iok]

    ivar <- awsDAT$variable %in% awsVAR$tahmo_code
    tmp <- awsDAT[ivar, , drop = FALSE]
    tmp$time <- as.POSIXct(temps[ivar])
    tmp <- tmp[!is.na(tmp$value), , drop = FALSE]

    if(nrow(tmp) == 0) return(NULL)

    ix <- match(tmp$variable, awsVAR$tahmo_code)

    tmp$value <- as.numeric(tmp$value)
    tmp$value <- tmp$value * awsVAR$multiplier[ix]

    var_nm <- c("var_height", "var_code", "stat_code")
    tmp <- cbind(awsVAR[ix, var_nm, drop = FALSE],
                 time = as.numeric(tmp$time),
                 value = tmp$value)
    ######
    tmp$network <- awsNET
    tmp$id <- awsID
    tmp$limit_check <- NA

    ######
    tmp <- tmp[, c("network", "id", "var_height",
                   "var_code", "stat_code",
                   "time", "value", "limit_check")]

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)
    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- c("network", "id", "height", "var_code",
                    "stat_code", "obs_time", "value", "limit_check")

    return(tmp)
}
