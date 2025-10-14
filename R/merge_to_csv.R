#'
#'This is a modification of the original uds_to_csv function to do a bit more than just prep a CSV
#'We'll need to generalize this a bit once UDS4 comes in, for now we have form_ver as an argument
#'

merge_to_csv <- function(.mim, .redcap, csv_string,
                         mim_dict, form_map, .type,
                         .mim_backup = NULL,
                         id_col = "adc_sub_id", match_col = "PET",
                         by_cols = c("adc_sub_id", "Image_visit"),
                         match_dict = date_col_match,
                         remap_dict = redcap_dict[["merge_remap"]]
                         ){

  #This used to be where we identified whether we iterate over the uds_form_map or the uab_imaging_map but now it's just a required arguemnt in form_map
  #Only two but we've allowed for more by making it a non-default argument


  #In preparation of UDS4, we want to iterate over our possible form versions

  uds_redcap_list <- lapply(form_map[["map"]], function(.form_ver){

    #First pull the appropriate entry from the MIM list (this goes first so we can get the IDs)
    .mim <- .mim[[.form_ver]]
    if(is.null(.mim)) return(NULL)

    #Next is subsetting the redcap data by the MIM data according to form version and ID
    #The form_ver_num map only applies to UDS version since only a single entry is expected for the general imaging list
    if(!is.na(form_map[["redcap_col"]])){
      .redcap <- .redcap[.redcap[[form_map[["redcap_col"]]]] == names(form_map[["map"]])[which(form_map[["map"]] == .form_ver)],]
      if(nrow(.redcap) == 0) return(NULL)
    }
    .redcap <- .redcap[.redcap[[id_col]] %in% .mim[[id_col]],]
    if(nrow(.redcap) == 0) return(NULL)



    #Before we index, we want to fill add in any missing columns specific to the UDS version
    #Technically we can do this whenever so long as we're within a UDS form version but it's nice to do it now so we can reorder columns
    #First identify any missing columns
    missing_uds <- mim_dict[[.form_ver]][["quest_id"]][mim_dict[[.form_ver]][["quest_id"]] %not_in% colnames(.mim)]
    if(length(missing_uds) > 0){

      #Quick lapply to populate each missing_uds column according to the default response, we rep based on the rows of .mim
      #We either step through each answer or just do blanket substitution if default response is only length 1
      uds_fill <-  lapply(missing_uds, function(.uds_col){
        if(length(mim_dict[[.form_ver]][["default_response"]]) == 1){
          rep(mim_dict[[.form_ver]][["default_response"]], nrow(.mim))
        } else{
          rep(mim_dict[[.form_ver]][["default_response"]][which(mim_dict[[.form_ver]][["quest_id"]] == .uds_col)], nrow(.mim))
        }
      })

      #Bind the list and name the columns
      uds_fill <- do.call(cbind.data.frame, uds_fill)
      # Ensure uds_fill is a data frame with proper dimensions before assigning colnames
      if(is.null(dim(uds_fill)) || length(dim(uds_fill)) < 2) {
        uds_fill <- as.data.frame(uds_fill)
      }
      colnames(uds_fill) <- missing_uds

      #Bind the .mim and missing_uds dataframe together and reorder (we do this as a data frame instead of data.table)
      .mim <- cbind.data.frame(.mim, uds_fill)
      .mim[, colnames(.mim) %in% mim_dict[[.form_ver]][["quest_id"]]] <-
        .mim[,match(mim_dict[[.form_ver]][["quest_id"]], colnames(.mim))]

      #For some reason, the columns are reordered but the names aren't, we just add that as an additional step  - make sure the output is still a data.table for the merge in the next step
      # Ensure .mim is a proper data frame before assigning colnames
      if(is.data.frame(.mim) && ncol(.mim) > 0) {
        colnames(.mim)[colnames(.mim) %in% mim_dict[[.form_ver]][["quest_id"]]] <- mim_dict[[.form_ver]][["quest_id"]]
      }
      .mim <- data.table::as.data.table(.mim)

    }


    #This is the big matching step to combine the .mim and .redcap data frames together
    #To bind the two dataframes together, we can identify which rows of .redcap match to .mim and apply those to .mim
    #We can implement this most easily by stepping through the same ID/Visit checks we've used before
    #See mim_redcap_row_match below for details on the process
    .mim[, redcap_idx := mim_redcap_row_match(.SD, .BY, .redcap), by = by_cols]

    #Drop NA's and make sure the indexing is properly ordered
    .mim <- .mim[!is.na(.mim$redcap_idx),]
    .mim <- .mim[order(.mim$redcap_idx),]


    #With the indexing complete, we can use the redcap_idx vector to subset the .redcap data and bind everything together
    .redcap <- .redcap[.mim$redcap_idx,]


    #We can just bind everything together by virtue of having ordered .mim based on the redcap_idx
    .redcap <- cbind.data.frame(.redcap[,colnames(.redcap) %in% remap_dict[[.type]]], .mim[,colnames(.mim) %in% mim_dict[[.form_ver]][["quest_id"]], with = FALSE])

    return(.redcap)

  })

  #With each form version processed, we can now bind out the data.frame
  redcap_out <- do.call(plyr::rbind.fill, uds_redcap_list)

  # Ensure redcap_out is a proper data frame before proceeding
  if(is.null(redcap_out) || nrow(redcap_out) == 0) {
    return(data.frame())
  }

  redcap_out[is.na(redcap_out)] <- ""

  #Final step is to remap column name like nacc_record back to the redcap expected output
  for(.idx in seq_along(remap_dict[[.type]])){
    if(length(remap_dict[[.type]]) > 0 && .idx <= length(remap_dict[[.type]])) {
      colnames(redcap_out)[which(colnames(redcap_out) == remap_dict[[.type]][[.idx]])] <-
        names(remap_dict[[.type]])[[.idx]]
    }
  }

  # Apply imaging-specific cleanups
  if("imagwmhsev" %in% colnames(redcap_out) && "imagmwmh" %in% colnames(redcap_out)){
    # imagwmhsev only valid if imagmwmh == 1; otherwise blank
    suppressWarnings({
      invalid_idx <- which(as.character(redcap_out$imagmwmh) != "1" | is.na(redcap_out$imagmwmh))
    })
    if(length(invalid_idx) > 0) redcap_out$imagwmhsev[invalid_idx] <- ""
  }

  #Write to file
  name_out <- paste0(csv_string, lubridate::today(), ".csv")
  if(file.exists(name_out)) file.remove(name_out)
  write.csv(redcap_out, name_out, quote = FALSE, row.names = FALSE)

  return(redcap_out)
}




#This is the primary function that matches the .mim data to the .redcap data
#It is designed to return an indexing based on matches according to date and ID


mim_redcap_row_match <- function(.mimdat, .by_id, .red,
                                  id_col = "adc_sub_id", match_dict = date_col_match, match_col = "PET",
                                 force_latest = TRUE){

  #Get the current ID from .by_id
  .id <- .by_id[[id_col]]

  #Subset the redcap data by the current id
  .red_id <- .red[.red[[id_col]] == .id,]

  #First make sure an ID was found in REDCap (will return a 0 row dataframe if not)
  if(is.data.frame(.red_id) && nrow(.red_id) == 0){
    print(paste0("ADC ID ", .id, " not found in REDCap data; returning NA"))
    return(NA_integer_)
  }



  #Get the current date matching entry we need and subset the .mim row we're working on
  .match_dict_entry <- match_dict[[.mimdat[[match_col]]]]
  .mim_match <- data.frame(.mimdat[,c(names(.match_dict_entry)[names(.match_dict_entry) %in% colnames(.mimdat)]),with=FALSE])
  .mim_match <- cbind.data.frame(.id, .mim_match)
  colnames(.mim_match)[1] <- id_col
  .mim_match_cols <- colnames(.mim_match)


  #Coerce everything in mim_match to a character for ease of comparison
  #Need to use lapply since it's a single row, it just behaves oddly even as a data.table
  .mim_match <- as.data.frame(lapply(.mim_match, as.character))
  colnames(.mim_match) <- .mim_match_cols

  #Step through each row in the REDCap visit dictionary after also converting .red_match to a character
  .red_match <- as.data.frame(sapply(.red[,colnames(.red) %in% c(id_col, match_dict[["uds_dt"]], .match_dict_entry)], as.character))

  #We use the compare function with allowAll set to TRUE to compare the elements or .mim_match and .red_match
  matched_vector <-
    apply(.red[,colnames(.red) %in% c(id_col, .match_dict_entry)], 1, function(.row){
    compare::compare(.row, .mim_match, allowAll = TRUE)[["result"]]
    })



  #If it's STILL more than 1 row, then we go ahead and do the sanity checks from before
  #Return NA if no matches or multiple matches are found
  if(sum(matched_vector) == 0 | sum(matched_vector) > 1 & isFALSE(force_latest)) {
    print(writeLines("Issue with matches on Dates and IDs in REDCap data - returning NA\n"))
    print(writeLines(paste0("Match count for ", .id, " is ", sum(matched_vector), "\n\n")))
    print(.mimdat)
    print(writeLines("\n\nREDCap data"))
    .red_error <- .red_id[.red_id[[id_col]] == .id,]
    if(nrow(.red_error) == 0){print("No ID in REDCap pull")
    } else print(.red_error)
    print(writeLines("\n\n\n\n"))
    return(NA_integer_)
  } else if(sum(matched_vector) == 0 | sum(matched_vector) > 1){
    print(writeLines("Issue with matches on Dates and IDs in REDCap data - returning NA\n"))
    print(writeLines(paste0("Match count for ", .id, " is ", sum(matched_vector), "\n\n")))
    print(.mimdat)
    print(writeLines("\n\nREDCap data"))
    print("Forcing to latest matched date")
    print(writeLines("\n\n\n\n"))
    print(writeLines("\n\n\n\n"))
    return(max(which(matched_vector == TRUE)))
  }

  #Otherwise return the index where a match was found
  return(which(matched_vector == TRUE))
}








