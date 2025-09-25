#Step 4 - add header info for redcap and print to file

uds_to_csv <- function(.uds, .head, .event){

  #Prep the PTID and event name for REDCap upload
  .head <- data.frame(.head[["adc_sub_id"]], .event)
  colnames(.head) <- c(redcap_id, redcap_event)
  .uds <- data.frame(.head, .uds)

  #Write to file
  name_out <- paste0("redcap_input_", lubridate::today(), ".csv")
  write.csv(.uds, name_out, quote = FALSE, row.names = FALSE)

  return(TRUE)
}
