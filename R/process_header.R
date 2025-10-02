#Step 2 - Process the header to get PTID, analyst and date
#This may need to be cross-referenced with MIM_header_names to make sure the right column names are use

#Also, update the gsub_clean default if necessary depending on how meta-data format changes


process_header <- function(.head, gsub_clean=".*: ?"){

  #Validate the length of the header against the stock MIM names, adjust if needed
  if(length(.head) != length(MIM_header_names)) {
    print("MIM header length does not equal length of names, check MIM_header_names")
    return(NULL)
  }

  #Chop meta data (everything before the colon)
  .head <- lapply(.head, function(xx) gsub(gsub_clean, "", xx))
  .head <- data.frame(.head)

  #Name accordingly and return
  names(.head) <- MIM_header_names

  #Make sure date is an actual date in 'mdy' format
  .head[["dt"]] <- lubridate::date(lubridate::as_date(.head[["dt"]]))

  return(.head)
}










