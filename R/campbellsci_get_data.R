
get.campbellsci.data <- function(dirAWS, dirCBS){
    options(scipen = 999)
    tz <- "Africa/Bamako"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    awsNET <- 3

    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "minutes", "CAMPBELLSCI")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "CAMPBELLSCI")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_campbellsci_", mon, ".txt"))
    awsLOG <- file.path(dirLOG, paste0("AWS_LOG_", mon, ".txt"))

    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "campbellsci_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                  stringsAsFactors = FALSE, quote = "\"",
                                  fileEncoding = "latin1")
    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "campbellsci_lastDates.csv")
    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE, quote = "\"",
                                 fileEncoding = 'latin1')

    dirDATA <- file.path(dirCBS, "DATA")

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        pattern <- paste0('^', awsID, '_', '.+\\.dat$')
        aws_list <- list.files(dirDATA, pattern)

        if(length(aws_list) == 0) next

        awsL <- strsplit(aws_list, "_")
        len <- sapply(awsL, 'length')
        ilen <- len == 3
        if(!any(ilen)) next

        aws_list <- aws_list[ilen]
        awsL <- awsL[ilen]
        awsL <- do.call(rbind, awsL)
        awsL[, 3] <- gsub('\\.dat', '', awsL[, 3])

        start <- strptime(awsL[, 2], '%Y%m%d%H', tz)
        end <- strptime(awsL[, 3], '%Y%m%d%H', tz)

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
            header <- utils::read.table(text = readLines(aws_file, n = 2, encoding = 'UTF-8')[2], sep = ',')
            header <- as.character(header[1, ])
            awsDAT <- utils::read.table(aws_file, sep = ',', header = FALSE, skip = 4,
                                        na.strings = "NAN", stringsAsFactors = FALSE, quote = "\"",
                                        fileEncoding = 'latin1', skipNul = TRUE)
            names(awsDAT) <- header
            out <- try(parse.campbellsci.data(awsDAT, header, awsVAR, awsID, awsNET), silent = TRUE)
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

parse.campbellsci.data <- function(awsDAT, header, awsVAR, awsID, awsNET){
    tz <- "Africa/Bamako"
    Sys.setenv(TZ = tz)

    temps <- strptime(awsDAT$TIMESTAMP, "%Y-%m-%d %H:%M:%S", tz = tz)
    ina <- is.na(temps)
    awsDAT <- awsDAT[!ina, , drop = FALSE]
    if(nrow(awsDAT) == 0) return(NULL)

    temps <- temps[!ina]
    temps <- as.numeric(temps)

    vcol <- match(awsVAR$cbs_lab, header)
    tmp <- awsDAT[, vcol, drop = FALSE]

    tmp <- lapply(seq(length(vcol)), function(i){
        data.frame(network = awsNET,
                   id = awsID,
                   height = awsVAR$var_height[i],
                   var_code = awsVAR$var_code[i],
                   stat_code = awsVAR$stat_code[i],
                   obs_time = temps,
                   value = tmp[, i])
    })
    tmp <- do.call(rbind, tmp)
    tmp$limit_check <- NA

    tmp$value <- as.numeric(tmp$value)
    tmp <- tmp[!is.na(tmp$value), , drop = FALSE]

    if(nrow(tmp) == 0) return(NULL)

    name_tmp <- names(tmp)
    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)
    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- name_tmp

    return(tmp)
}
