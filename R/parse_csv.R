#'
#'
#' Step 1 - read in the csv sets to a list, return the file and header information as well as a list of tables
#' The goal here is simply standardization of the input, actual processing into a REDCap format can come later
#'



parse_csv <- function(input_files, csv_table_split = "==", csv_dict = csv_list_dict){

  csv_out <-
    lapply(input_files, function(.csv){

      #Parse out the name to get ADC ID, date and method - Print for reference
      file_data <- header_name_parser(.csv, dict = parsing_dict[["file_name"]])

      #Print the output of file_data for QC checks based on some criteria
      if(ncol(file_data) != length(parsing_dict[["file_name"]][["col_names"]]) || #Not enough columns pulled by header_name_parser
         ncol(file_data[,!is.na(file_data)]) != ncol(file_data) ||   #Any NA's are found
         length(grep("AV(\\d)+|PIB|Tau|Abeta", file_data[["PET"]], ignore.case = TRUE)) == 0){  #PET name not recognized
           print(file_data)
           writeLines("\n")
      }

      #Pull in the current CSV to break it into the constituent parts
      csv_curr <- read.csv(.csv, header = FALSE, check.names = FALSE)

      #Split the csv into a list based on the equals, this identifies the rows we ultimately want to drop
      table_split <- c(1, grep(csv_table_split, csv_curr[,1]))

      #Remove any consecutive values in table_split since it means you have adjoining equals signs
      table_split_drop <- diff(table_split)
      if(any(table_split_drop==1)){
        table_split <- table_split[-which(table_split_drop==1)]
      }

      csv_list <- lapply(seq_along(table_split), function(.start){

        #Since we use seq_along .start is an index variable, just good to keep in mind

        #Get the ending point, we move up by one so we don't capture the table split row
        if(.start == length(table_split)){ .end <- nrow(csv_curr)
        } else .end <- table_split[.start + 1] - 1

        #Get the start point, again if it's not the first row we shift down by one to avoid the table split row
        .start <- table_split[.start]
        if(.start != 1) .start <- .start + 1
        .out <- csv_curr[c(.start:.end),]

        #Final clean to drop any csv_table_split rows
        .out[grep(csv_table_split, .out[,1], invert=TRUE),]
      })


      #From the list of CSVs, extract the header file and drop any null columns
      csv_list <- lapply(csv_list, function(.tab){

        #Step 1 is to get rid of any carriage returns
        .tab <- as.data.frame(purrr::map_dfc(.tab, function(xx){gsub("\\\n|\\\t", "", xx)}))

        #We set up a while loop to pull header rows until a valid column name field is found in the first cell
        pulling_headers <- TRUE; header_out <- NULL
        valid_cols <- do.call(c, csv_dict[which(names(csv_dict) %in% c("PET_col", "Vascular_col", "Volumes_col"))])

        while(pulling_headers == TRUE){
          #Check if the first cell of the current row corresponds to a valid column name in csv_list_dict
          if(.tab[1,1] %in% valid_cols){ pulling_headers <- FALSE

          #Otherwise process the row as valid header information before dropping the first row from the table
          } else{
            header_curr <- .tab[1,]
            header_curr <- header_curr[, colSums(is.na(header_curr)) != nrow(header_curr)]
            header_curr <- header_name_parser(header_curr, dict = parsing_dict[["header"]])

            # If the current header row does not yield any parsed fields (new-form rows),
            # skip binding to avoid differing number of rows during cbind.
            if(!is.null(header_curr) && nrow(header_curr) > 0){
              if(is.null(header_out)){ header_out <- header_curr
              } else header_out <- cbind.data.frame(header_out, header_curr)
            }
            .tab <- .tab[-1,]
          }
        }

        #Remap header columns to be either TAU or PIB specific; include remap as needed
        header_annotate <- tolower(file_data[["PET"]])
        if(length(grep("av", header_annotate))>0) header_annotate <- "tau"
        if(length(grep("pib", header_annotate))>0) header_annotate <- "abeta"
        colnames(header_out)[colnames(header_out) %in% parsing_dict[["annotate"]]] <-
          paste0(colnames(header_out)[colnames(header_out) %in% parsing_dict[["annotate"]]], "_", header_annotate)

        #With all header rows pulled, push the remaining row (which has valid column names) into the column name position
        colnames(.tab) <- .tab[1,]
        .tab <- .tab[-1,]

        #Drop any columns that are nothing but blanks because of uneven rows between tables
        col_idx <- lapply(.tab, function(.col){
          if(length(.col) == length(which(.col == ""))) {return(FALSE)
          } else return(TRUE)
        })
        col_idx <- do.call(c, col_idx)
        .tab <- .tab[,col_idx]


        #As a final check, pulling any tail rows, the most likely for now being
        tail_check <- grep(parsing_dict[["tail"]][["row_ident"]], .tab[,1])
        if(length(tail_check) > 0){
          tail_curr <- lapply(tail_check, function(.tail){
            header_name_parser(.tab[.tail,], dict = parsing_dict[["tail"]])
          })
          # Drop NULLs and zero-row frames
          tail_curr <- purrr::compact(tail_curr)
          if(length(tail_curr) > 0){
            tail_curr <- tail_curr[sapply(tail_curr, function(tt) is.data.frame(tt) && nrow(tt) >= 1)]
          }
          if(length(tail_curr) > 0){
            # Ensure single-row per component
            tail_curr <- lapply(tail_curr, function(tt){ if(nrow(tt) > 1) tt[1,,drop=FALSE] else tt })
            tail_curr <- do.call(cbind.data.frame, tail_curr)
            # Only bind tail rows if we have an initialized header_out; otherwise start it
            if(is.null(header_out)){
              header_out <- tail_curr
            } else {
              header_out <- cbind.data.frame(header_out, tail_curr)
            }
          }
          .tab <- .tab[-tail_check,]
        }


        #Finally return the processed header and the current .tab
        return(list(header = header_out, table = .tab))

      })

      #With the tables fully processed, take the full selection of header data to a single row and add the file data
      headers_list <- lapply(csv_list, function(.csv){.csv[["header"]]})
      # Drop NULLs and zero-row frames to prevent row count mismatches
      headers_list <- purrr::compact(headers_list)
      if(length(headers_list) > 0){
        # Drop any zero-row header frames
        headers_list <- headers_list[sapply(headers_list, function(hh) is.data.frame(hh) && nrow(hh) >= 1)]
      }
      if(length(headers_list) > 0){
        headers_list <- lapply(headers_list, function(hh){ if(nrow(hh) > 1) hh[1,,drop=FALSE] else hh })
        header_out <- cbind.data.frame(file_data, do.call(cbind.data.frame, headers_list))
      } else {
        # If no headers parsed, just use file_data as the header row
        header_out <- file_data
      }

      # Force new forms to UDS4
      header_out$uds_version <- "UDS4"


      #At this point we've returned a list with each processed table from MIM along with an associated header
      #Name the list, always assume it's the right number of entries, adjust the dictionary as necessary
      csv_list <- lapply(csv_list, function(.csv){.csv[["table"]]})
      names(csv_list) <- csv_dict[["key"]]


      return(list(header = header_out, tables = csv_list))
    })


  return(csv_out)
}







header_name_parser <- function(.dat, dict, df_collapse = TRUE){

  #Coerce a dataframe to a string if needed, separate using underscore and tack an extra one at the end just in case
  if(is.data.frame(.dat) && df_collapse == TRUE) .dat <- paste0(paste(.dat, collapse = "_"), "_")

  #Step through the dictionary, extract the relevant portions
  parse_out <- lapply(seq_along(dict$col_names), function(.idx){
    .out <- gsub(dict[["gsub_string"]][.idx], "\\1", .dat, ignore.case = TRUE)

    #If no match is made (wherein gsub returns the original string), return null
    if(.out == .dat) return(NULL)

    #Process as a date if needed
    if(!is.na(dict[["date_string"]][.idx])){
      .out <- do.call(getfunc(dict[["date_string"]][.idx]), list(.out))
    }

    #Assuming a valid extraction was made, name the entry and return
    #First make a call to toupper in case there was a vertical bar in the gsub_string (this might impact Image_visit for example)
    if(length(grep("\\(.|.\\)", dict[["gsub_string"]][.idx])) > 0) .out <- toupper(.out)
    .out <- data.frame(.out)
    names(.out) <- dict[["col_names"]][.idx]
    return(.out)

  })

  #Pass to dataframe and return
  parse_out <- purrr::compact(parse_out)
  parse_out <- do.call(cbind.data.frame, parse_out)

  return(parse_out)

}
