#'
#'
#' The function to merge rows of different imaging modalities
#' Also checks whether that are discrepancies in NACC measures
#'



merge_nacc_rows <- function(.data_orig, mim_dict, by_vector = c("adc_sub_id", "Image_visit"), force_latest = FALSE, enforce_max = FALSE){

  #See row_merge() for new argument force_latest which defaults to the most recent image scan if there's a mismatch on UDS questions
  ##For details, look under `if("Mismatch" %in% .compare_idx)` section
  #Default is to NOT force the latest result but instead ust print the warning error and return NULL

  #To account for UDS versions we iterate over the entries in mim_dict
  uds_version_list <- lapply(names(mim_dict), function(.form_ver){

    #Get the current dictionary
    mim_dict_curr <- mim_dict[[.form_ver]]

    # #Get the current nacc data based on uds version, return null if none exist
    # if(!is.na(mim_dict_curr[["uds_ver_col"]])){
    #   .data <- .data_orig[.data_orig[[mim_dict_curr[["uds_ver_col"]]]] == .form_ver,]
    # } else{
    #   .data <- .data_orig
    # }
    .data <- .data_orig[[.form_ver]]
    if(nrow(.data) == 0) return(NULL)

    #Coerce to data.table
    .data <- data.table::as.data.table(.data)

    #Step through according to the pairings of ADC ID and the Image Visit and call the row_merge internal function
    .data_out <- .data[,row_merge(.SD, .BY, mim_dict = mim_dict_curr, .latest = force_latest, .enforce_max = enforce_max), by = by_vector]

    #With the merge complete, we rename the question numbers to the expected REDCap names and return
    recode_vector <- mim_dict_curr[["quest_id"]]
    names(recode_vector) <- mim_dict_curr[["quest_num"]]
    colnames(.data_out) <- dplyr::recode(colnames(.data_out), !!!recode_vector)

    #Finally drop any columns not in the remap
    .to_drop <- which(colnames(.data_out) %in% mim_dict_curr[["temp_drop"]])
    if(length(.to_drop) > 0){
      .data_out <- .data_out[,-.to_drop,with = FALSE]
    }

    #Also force any integer columns (Fazekas, Duara) as integers
    if(sum(colnames(.data_out) %in% mim_dict_curr[["force_integer"]]) > 0){
      .data_out[, (mim_dict_curr[["force_integer"]]) := lapply(.SD, as.integer), .SDcols = mim_dict_curr[["force_integer"]]]
    }

    # return(list(.form_ver, .data_out))
    return(.data_out)
  })

  #We just name the list here since we rely on the list structure
  names(uds_version_list) <- names(.data_orig)

	#Drop any null columns, extract the names for indexing in the next step, create the final list to return
	null_uds <- which(sapply(uds_version_list, is.null))
	if(length(null_uds) > 0) uds_version_list <- uds_version_list[-null_uds]
	# uds_version_names <- do.call(c, lapply(uds_version_list, function(.list){.list[[1]]}))
	# uds_version_list <- lapply(uds_version_list, function(.list){.list[[2]]})
	# names(uds_version_list) <- uds_version_names

  return(uds_version_list)


}



#This is the primary internal function called by the data.table to match and merge rows
#As usual, we need to be mindful of pulling column names in a data.table object
#Since we're using lists as dictionary we generally have to use with = FALSE instead of .. conventions (.. requires a vector, not a list slot)
#We also can't use .. since we may end up with missing columns if they aren't in the .dat object
#This gets tricky when assigning things during the remap though
#For some reason, with=FALSE doesn't seem to work within the j function call
#We just take advantage of the face .dat_remap has column names and use those when reassigning variable names, see line 45 for an example

#5/25/23 - New inclusion of .latest which forces any mismatched questions to the most recent scan
#For details, look under `if("Mismatch" %in% .compare_idx)` section

row_merge <- function(.dat, .id, mim_dict, .latest = FALSE, .enforce_max = FALSE){

  #If there's only one row, simply recast the null values and call it good
  #We can't use recode since it's doesn't play well across columns and it also requires exhaustive remap definitions
  #we'd also need to specify everything so using mutate isn't any more efficient
  #Instead we just iterate through our recode vector in the dictionary which is comparatively fast
  if(nrow(.dat) == 1){
    .dat_remap <- .dat[1, colnames(.dat) %in% mim_dict$quest_num, with = FALSE]
    for(ii in seq_along(mim_dict$uds_recode)){
      .dat_remap[.dat_remap == mim_dict$uds_null[ii]] <- mim_dict$uds_recode[ii]
    }
    #This is the remap assignment for data.table we described above
    .dat[1, colnames(.dat_remap) ] <- .dat_remap
    return(.dat)
  }

  #Subset to get the NACC questions to compare
  .dat_compare <- .dat[,colnames(.dat) %in% mim_dict$quest_num, with = FALSE]

  #Our comparison check steps through the columns to return a vector
  .compare_idx <- lapply(.dat_compare, function(.col){

    #We need to account for columns that may have NA's, this is especially an issue with the Imaging dictionary since rbind.fill combines very disparate tables
    #The easiest solution (WHICH MAY NOT ALWAYS WORK) will be to replace NA's with the corresponding null value
    .col[is.na(.col)] <- mim_dict$uds_null

    #Check to see if the uds_recode and an actual uds_recode value exists and replacing the recode with null
    if(length(unique(.col[!(.col %in% mim_dict$uds_null) & !(.col %in% mim_dict$uds_recode)])) > 0 &&
       length(unique(.col[!(.col %in% mim_dict$uds_null) & (.col %in% mim_dict$uds_recode)])) > 0){
      .col[.col %in% mim_dict$uds_recode] <- mim_dict$uds_null
    }

    #Reduced columns like STUDY INFORMATION behave badly since they're essentially guaranteed to mismatch to a degree
    #We can check for the collapse string and reconstitute it instead
    if(!is.na(mim_dict$reduce_collapse_string) && length(grep(mim_dict$reduce_collapse_string, .col)) > 0){
      .reduc_col <- paste(.col, collapse = mim_dict$reduce_collapse_string)
      .reduc_col <- stringr::str_split(.reduc_col, mim_dict$reduce_collapse_string)[[1]]  #Need to extract as a list entry since stringr::str_split returns a list from vectorization
      .reduc_col <- sort(unique(.reduc_col))
      .reduc_col <- paste(.reduc_col, collapse = mim_dict$reduce_collapse_string)
      return(.reduc_col)
    }

    #A special catch for mim_neuro_id which is expected to be different across rows but is uploaded so is found in dict[["quest_num"]]
    #To identify this column we regex the expected pattern
    if(length(grep(".*?(PIB|AV1451).*?(X|x)\\d+", .col)) > 0) return(data.frame(paste(.col, collapse = " / ")))


    #Now we can get to the actual mismatch checking
    #First check if there are more than two unique values OR there are two values but none of them are a valid NULL
    if(length(unique(.col)) > 2 || (length(unique(.col)) == 2 && mim_dict$uds_null %not_in% .col)){
      return("Mismatch")

    #Next if there's only one unique value, just return it whatever it is
    } else if(length(unique(.col)) == 1){
      return(unique(.col))

    #Otherwise, return whatever the non-null value is
    } else{
      return(unique(.col[.col %not_in% mim_dict$uds_null]))
    }
  })

  #Bind the list to a vector
  .compare_idx <- do.call(cbind.data.frame, .compare_idx)
  colnames(.compare_idx) <- colnames(.dat_compare)

  #Finally, check if a mismatch occurs
  if("Mismatch" %in% .compare_idx){

    #If we want to force the results of the most recent scan...
    if(isTRUE(.latest)){

      writeLines(paste0("\nMismatch found - UDS responses - check following entry - ", paste(.id, collapse = ": ")))
      print(.dat[, colnames(.dat) %in% colnames(.compare_idx)[.compare_idx == "Mismatch"],with = FALSE])
      writeLines("Forcing values from latest row, change force_latest and enforce_max arguments to FALSE to skip")

      #First find the row with the most recent date
      #Pull the data columns based on the date_col_match dictionary
      .dat_date <- .dat[,colnames(.dat) %in% names(date_col_match[["All"]]), with = FALSE]
      #Get the most recent date from each row
      .dat_date <- apply(.dat_date, 1, function(.row){
        if(all(is.na(.row))) { return(NA)
        } else max(na.omit(as.Date(.row)))
      })
      #And then get the index of the most recent date of the resulting vector (we just take the first of the max() in case there's a tie)
      .dat_date_idx <- which(.dat_date == max(.dat_date))[1]

      #Now that we know the index, we extract those mismatch values and fill in .compare_idx
      .compare_idx[,.compare_idx == "Mismatch"] <- .dat[.dat_date_idx, colnames(.dat) %in% colnames(.compare_idx)[.compare_idx == "Mismatch"],with = FALSE]

    } else if(isTRUE(.enforce_max)){

      writeLines(paste0("\nMismatch found - UDS responses - check following entry - ", paste(.id, collapse = ": ")))
      print(.dat[, colnames(.dat) %in% colnames(.compare_idx)[.compare_idx == "Mismatch"],with = FALSE])
      writeLines("Enforcing maximum values for all rows, change force_latest and enforce_max arguments to FALSE to skip")

      #For the data where a mismatch is observed, simply pull the maximum value
      .dat_date <- sapply(.dat_date, function(.col){
        if(all(is.na(.col))) { return(NA)
        } else if(length(unique(.col))==1) {return(unique(.col))
        } else return(max(na.omit(.col[!(.col %in% mim_dict$uds_recode)])))
      })

    }else{

      #As an alternative, just print the warning and return NULL to prevent further processing
      writeLines(paste0("\nMismatch found - UDS responses - returning NULL - check following entry - ", paste(.id, collapse = ": ")))
      print(.dat[, colnames(.dat) %in% colnames(.compare_idx)[.compare_idx == "Mismatch"],with = FALSE])
      return(NULL)

    }

  }




  #So now .compare_idx is a single vector of values to replace, but we also want to consolidate the other columns which will have a number of NA's
  .dat_merge <- lapply(colnames(.dat[,colnames(.dat) %not_in% mim_dict$quest_num, with = FALSE]), function(.col_name){
    .col_out <- unique(.dat[[.col_name]][!is.na(.dat[[.col_name]]) & .dat[[.col_name]] %not_in% c("", mim_dict$uds_null)])

    #If length 0 then everything is NA, return a blank value
    if(length(.col_out) == 0){
      .out <- data.frame("")
      colnames.out <- .col_name

    #A special catch for PET which is expected to have multiple columns; similar to mim_neuro_id although PET isn't an uploaded field so it's handled in this merge step
    }else if(.col_name == "PET"){ .out <- data.frame("All")

    #Return NULL if more than one value is found (will be caught during a column match later)
    }else if(length(.col_out)!=1){ return(NULL)

    #Otherwise return the non-NA / non-Null value
    } else .out <- data.frame(.col_out)

    #Give it the same column name and return
    colnames(.out) <- .col_name
    return(.out)
  })

  #Merge and make sure the columns match (which will be false if NULL was returned due to multiple merge values in the previous lapply)
  null_merge <- which(sapply(.dat_merge, is.null))
  if(length(null_merge) > 0) .dat_merge <- .dat_merge[-null_merge]
  .dat_merge <- do.call(cbind.data.frame,.dat_merge)
  if(ncol(.dat_merge) != ncol(.dat[,colnames(.dat) %not_in% mim_dict$quest_num, with = FALSE])){
    writeLines("\nMismatch found - non UDS column names - returning NULL - check following entry - ", paste(.id, collapse = ": "))
    print(.dat)
    return(NULL)
  }




  #Assuming the matching (.compare_idx) and merging (.dat_merge) were both successful, rebind the data.frame, reorder the columns
  .dat_final <- cbind.data.frame(.dat_merge, .compare_idx)
  #We recast it back to a data.table here since it may mess with the stock remap code we use early on,
  .dat_final <- data.table::as.data.table(.dat_final[,order(match(colnames(.dat_final), colnames(.dat)))])

  #Finally, do the same remapping as if there were only one row
  .dat_remap <- .dat_final[1, colnames(.dat_final) %in% mim_dict$quest_num, with = FALSE]
  for(ii in seq_along(mim_dict$uds_recode)){
    .dat_remap[.dat_remap == mim_dict$uds_null[ii]] <- mim_dict$uds_recode[ii]
  }
  .dat_final[1,colnames(.dat_remap)] <- .dat_remap
  return(.dat_final)


}


