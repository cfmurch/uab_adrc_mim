#'
#' The functions to go from a list to a processed data frame
#' This is done prior to matching
#'
#'


process_csv <- function(.data_list, entry_form, dict = processing_table_dict){

  #We step through each entry in our processed list which corresponds to a participant specific MIM output
  dat_out <- lapply(.data_list, function(.mim){

    #As the first sanity check, make sure all study dates (i.e. any date column that isn't MIM_date are the same)
    date_check <- .mim$header[,grep("(?<!MIM)_date", colnames(.mim$header), perl = TRUE,)]
    if(length(unique(do.call(c, date_check))) != 1) {
      stop(print(list("Stopping process_csv - multiple study dates found in header:",.mim$header)))
    }

    #Begin by pulling the header material, namely the ID, the type of PET/Imaging
    header_curr <- .mim$header[,colnames(.mim$header) %in% dict[["header_cols"]]]
    #Drop any duplicate columns
    cols_to_drop <- grep("\\.\\d*$", colnames(header_curr))
    if(length(cols_to_drop) > 0){
      header_curr <- header_curr[,-cols_to_drop]
    }

    # Force new-form processing to UDS4
    header_curr$uds_version <- "UDS3"

    #Next step through the tables and extract the relevant columns
    table_processed <- lapply(.mim$tables, function(.table){

      #Step 1 - Get the relevant columns, return NULL if there's no column match (e.g. volumes when processing for D1)
      table_curr <- .table[,colnames(.table) %in% dict[[entry_form]][["table_cols"]]]
      if(ncol(table_curr) == 0) return(NULL)
      if(nrow(table_curr) == 0) return(NULL)

      #Step 2 check if the column with the names is going to be annotated by the type of PET
      if(!is.na(dict[[entry_form]][["annotate_pet_cols"]])){

        #We get the annotation value from the header_curr, this is almost always going to be PET but can be file dependent
        annotate_curr <- header_curr[[dict[[entry_form]][["annotate_pet_cols"]]]]

        #We identify which column in the table has our names
        col_with_var_names <-
          dict[[entry_form]][["var_with_col_names"]][which(dict[[entry_form]][["var_with_col_names"]] %in% colnames(.table))]

        #Then we annotate them together, this works even with PET specific entries like PIB since they get dropped in the next steps if they have a drop_val
        if(nrow(table_curr) == 0) return(NULL)
        table_curr[[col_with_var_names]] <- paste0(table_curr[[col_with_var_names]], "_", annotate_curr)
      }


      #Filter the rows by stepping through the appropriate drop_cols and filtering out rows with drop_values
      #We do this twice, once for standard two column tables and a 2nd time after making a long table as needed
      for(.idx in seq_along(dict[[entry_form]][["drop_cols"]])){
        .col <- dict[[entry_form]][["drop_cols"]][.idx]
        .drop <- dict[[entry_form]][["drop_values"]][[.idx]]  #Don't forget, drop_values is a list while drop_cols is a vector

        #If the column exists, drop values accordingly
        if(length(table_curr[[.col]]) > 0){
          table_curr <- table_curr[!is.na(table_curr[[.col]]) & table_curr[[.col]] %not_in% .drop,]
        }
      }
      if(nrow(table_curr) == 0) return(NULL)

      #Before we continue processing, we extract and reduction columns we may have
      #For now this is going to be the "STUDY INFORMATION" columns
      if(is.list(dict[[entry_form]][["reduction_vars"]]) && (dict[[entry_form]][["reduction_vars"]][["cols"]] %in% colnames(table_curr))){
        reduc_col <- lapply(dict[[entry_form]][["reduction_vars"]][["cols"]], function(.col){

          #Get unique values for what we're going to reduce and do any gsub adjustment by stepping through the gsub section of the list if needed
          var_curr <- unique(table_curr[[.col]])
          if(!is.na(dict[[entry_form]][["reduction_vars"]][["gsub_match"]])){
            var_curr <- lapply(seq_along(dict[[entry_form]][["reduction_vars"]][["gsub_match"]]), function(.string){
              gsub(dict[[entry_form]][["reduction_vars"]][["gsub_match"]][[.string]],
                                                                dict[[entry_form]][["reduction_vars"]][["gsub_replace"]][[.string]], var_curr)})
            var_curr <- do.call(c, var_curr)
          }

          #Finally, collapse to a single string and return the anticipated 2 column data frame (we'll combine these reduced values later)
          var_curr <- paste(var_curr, collapse = dict[[entry_form]][["reduction_vars"]][["collapse_string"]])
          return(data.frame(variable = .col, value = var_curr))
        })
        if(length(reduc_col) > 0) reduc_col <- do.call(rbind.data.frame, reduc_col)

        #As a slight addition, we can also expand reduc_col back out into four columns
        #This is just a gsub call to check for T1, T2, SWAN and PET in teh STUDY INFORMATION reduction
        reduc_col$value <- paste0("<br>", reduc_col$value, "<br>")
        expand_col <- lapply(seq_along(dict[[entry_form]][["reduction_vars"]][["expand"]]), function(.idx){
          #If one of the metadata entries are found via grep
          grep_check <- grep(dict[[entry_form]][["reduction_vars"]][["expand"]][[.idx]], reduc_col$value)
          if(length(grep_check) > 0){
            #Extract it using the gsub string in the expand entry of the dictionary
            .match <- gsub(dict[[entry_form]][["reduction_vars"]][["expand"]][.idx], "\\1", reduc_col$value, match, ignore.case = TRUE)
            #Or return blank
          } else .match <- NA
          return(.match)
        })
        #Coerce the lapply list to a data frame and name according to the dictionary names
        expand_col <- do.call(c, expand_col)
        expand_col <- data.frame(variable = names(dict[[entry_form]][["reduction_vars"]][["expand"]]), value = expand_col)

        #Finally, remap the expanded columns to be either TAU or PIB specific; same code chunk as in parse_csv
        header_annotate <- tolower(header_curr[["PET"]])
        if(length(grep("av", header_annotate))>0) header_annotate <- "tau"
        if(length(grep("pib", header_annotate))>0) header_annotate <- "abeta"
        expand_col$variable <- paste0(expand_col$variable, "_", header_annotate)

        #Drop NA's
        expand_col <- expand_col[!is.na(expand_col$value),]
      }

      #Now we can start working across the remaining columns to get to a table format if needed
      #This is largely based on whether we're extracting multiple disparate columns (e.g. for Imaging) or can easily bind the list together (e.g. the D1)
      #We identify this based on the length of var_with_col_names and var_with_data

      #If it's the former we essentially need to set it into a "variable" / "value" to column layout
      if(length(dict[[entry_form]][["var_with_col_names"]]) > 1 || length(dict[[entry_form]][["var_with_data"]]) > 1){

        #We do assume only one column for a given table is going to correspond to column names and these will be modified based on data
        variable_col <- dict[[entry_form]][["var_with_col_names"]][which(dict[[entry_form]][["var_with_col_names"]] %in% colnames(.table))]
        if(length(variable_col) != 1) {
          print("Too many columns with variable names indexed for current table")
          print(variable_col)
          print(table_curr)
          return(NULL)
        }
        colnames(table_curr)[[which(colnames(table_curr) == variable_col)]] <- "variable"

        #With the variable column identified, reduce the columns to those being used and pivot_longer
        data_cols <- dict[[entry_form]][["var_with_data"]][dict[[entry_form]][["var_with_data"]] %in% colnames(table_curr)]
        table_curr <- table_curr[,colnames(table_curr) %in% c("variable", data_cols)]
        if(nrow(table_curr) == 0) return(NULL)
        table_curr <- as.data.frame(tidyr::pivot_longer(table_curr, tidyselect::all_of(data_cols)))
        if(nrow(table_curr) == 0) return(NULL)
        table_curr$variable <- paste0(table_curr$variable, "_", table_curr$name)
        table_curr <- table_curr[,colnames(table_curr) %in% c("variable", "value")]
      }


      #Make sure the two column table is appropriately renamed if it didn't go through the previous steps
      if("variable" %not_in% colnames(table_curr) && "value" %not_in% colnames(table_curr)){
        # Identify actual present columns among candidates
        present_name_col <- dict[[entry_form]][["var_with_col_names"]][dict[[entry_form]][["var_with_col_names"]] %in% colnames(table_curr)]
        present_value_col <- dict[[entry_form]][["var_with_data"]][dict[[entry_form]][["var_with_data"]] %in% colnames(table_curr)]
        if(length(present_name_col) == 0 || length(present_value_col) == 0){
          return(NULL)
        }
        # If multiple, choose the first as variable source; keep both for values via pivot step above
        colnames(table_curr)[colnames(table_curr) == present_name_col[1]] <- "variable"
        # If multiple value columns and we didn't pivot (2-col table), choose the first as value
        if(length(present_value_col) > 1){
          colnames(table_curr)[colnames(table_curr) == present_value_col[1]] <- "value"
          # Drop any other value columns in 2-col mode
          table_curr <- table_curr[,colnames(table_curr) %in% c("variable","value")]
        } else {
          colnames(table_curr)[colnames(table_curr) == present_value_col] <- "value"
        }
      }
      if(nrow(table_curr) == 0) return(NULL)


      #Finally, add the reduced column data frame if it exists
      if(exists("expand_col")){
        if(nrow(table_curr) == 0){
          table_curr <- expand_col
        } else if(nrow(expand_col) == 0){
          table_curr <- table_curr
        } else{
          table_curr <- rbind.data.frame(table_curr, expand_col)
        }
      }

      return(table_curr)

    })


    #If applicable, process the header into its own data layout to be added to the final table
    if(dict[[entry_form]][["header_to_data"]] == TRUE){
      header_idx <- which(colnames(.mim$header) %in% dict[["header_to_data"]])
      if(length(header_idx) > 0) {
        header_data <- .mim$header[,header_idx]

        #Usual call to drop duplicate columns, as an aside, we can't use negative lookhead most likely because it's the end of the string
        cols_to_drop <- grep("\\.\\d*$", colnames(header_data))
        if(length(cols_to_drop) > 0) header_data <- header_data[,-cols_to_drop]

        #Coerce to the anticipated variable / value pairing
        header_data <- data.frame(variable = colnames(header_data), value = t(header_data))


        #Add the header data to the list for later binding
        table_processed[[length(table_processed) + 1]] <- header_data
      }
    }



    #With the table filtered on rows and columns, we now have a reduced data frame we can bind together
    table_reduc <- purrr::compact(table_processed)
    table_reduc <- do.call(rbind.data.frame, table_reduc)

    #We do a quick check to see if we have any duplicate variable names, this is most likely going to be a result of using reduced columns like STUDY INFORMATION
    if(length(table_reduc[["variable"]]) != length(unique(table_reduc[["variable"]]))){
      #Get the current duplicates
      duplicates_curr <- table_reduc[["variable"]][duplicated(table_reduc$variable)]
      for(ii in seq_along(duplicates_curr)){
        #Get the first instance, will initialize off this
        first_idx <- which(table_reduc$variable == duplicates_curr[ii])[1]

        #Check if there's more than one value in the duplicates variable names
        if(length(unique(table_reduc$value[table_reduc$variable == duplicates_curr[ii]])) > 1){
          #If soollapse all instances into the first_idx using the collapse string entry from the dictionary
          table_reduc$value[first_idx] <- paste(unique(table_reduc$value[table_reduc$variable == duplicates_curr[ii]]), collapse = dict[[entry_form]][["reduction_vars"]][["collapse_string"]])
        }

        #Finally, drop the duplicates from table_reduc
        table_reduc <- table_reduc[-which(table_reduc$variable == duplicates_curr[ii])[-1],]
      }
    }




    #We do get a deviation from the prior package because we need to recast to a wide data frame at this point
    #The use of multiple rows that need to be merged simply makes it easier to do the bind here
    #As a trade off, the csv_to_d1_uds function is really no longer applicable anymore and had to be reworked extensively
    #Instead we do the recoding here

    #Pull the current recode_key
    recode_key <- get0(dict[[entry_form]][["recode_key"]])

    #Recode the results using dplyr::recode (note, this requires the triple-bang on the recode_key for replacement to handle unquote splicing)
    table_reduc[["value"]] <- dplyr::recode(table_reduc[["value"]], !!!recode_key)

    #Run a second pass column drop if desired
    if(!is.na(dict[[entry_form]][["second_pass_drop"]])){
      table_reduc <- table_reduc[!is.na(table_reduc$value) & table_reduc$value %not_in% dict[[entry_form]][["second_pass_drop"]],]
    }
    table_reduc <- table_reduc[!is.na(table_reduc$value),]

    #With the recoding complete, pivot wider, add the columns names, then bind to the header
    table_out <- as.data.frame(t(table_reduc[["value"]]))
    colnames(table_out) <- table_reduc[["variable"]]
    table_out <- cbind.data.frame(header_curr, table_out)

    return(table_out)
  })



  #Compile the list of data frames into a single data frame (using rbind.fill to add missing columns) and return
  final_dat <- do.call(plyr::rbind.fill, dat_out)

  #As a final step (which is nit-picky), re-order the header based on the dictionary
  #After reordering, rebind the columns, this serves purely asthetic purposes for now
  final_dat_header <- final_dat[,colnames(final_dat) %in% dict[["header_cols"]]]

  #We also filter the dictionary column names prior to sorting just to avoid trying to match any undefined columns
  final_dat_header <- final_dat_header[,order(match(colnames(final_dat_header), dict[["header_cols"]][dict[["header_cols"]] %in% colnames(final_dat_header)]))]

  #Now bind everything back together to satisfy my OCD
  final_dat <- cbind.data.frame(final_dat_header, final_dat[,colnames(final_dat) %not_in% dict[["header_cols"]]])


  return(final_dat)
}



#The full dictionary set used during process_csv

processing_table_dict <- list(#header_cols are the columns will pull from the header entry
                              header_cols = c("adc_sub_id", "PET", "Tau_date", "Amyloid_date",
                                              "Image_visit", "uds_version"),

                              #This is the specialized dictionary to turn the header into a complement of the data file
                              header_to_data = list("MIM_date_abeta", "MIM_date_tau", "mim_neuro_id_abeta", "mim_neuro_id_tau",
                                                    "Analyzed_by_abeta", "Analyzed_by_tau", "amyl_suvr", "tau_score"),



                              #We subset the dictionary for table processing based on what form we're prepping for e.g. D1 or Imaging
                                #table_cols are the columns will pull from the set of tables (note this are dependent on which REDCap form we're importing to)
                                #drop_values are the rows we're going to be excluding, we nest as a list so we can allow for multiple drop_values with a given drop_cols
                                #drop_cols are the columns we apply drop_values to
                                #var_with_col_names tells us which column is going to be used for column names when we pivot_wider
                                #var_with_data in turn gives the data column

                              D1 = list(table_cols = c("RESULT", "NACC ITEM ID"),
                                        drop_cols = c("NACC ITEM ID"),
                                        drop_values = list(c("Not applicable")), second_pass_drop = NA,
                                        var_with_col_names = "NACC ITEM ID",
                                        var_with_data = "RESULT",

                                        recode_key = "uds_key",
                                        annotate_pet_cols = NA, header_to_data = FALSE,
                                        reduction_vars = NA),

                              Imaging = list(table_cols = c("IMAGING RESULT/PARAMETER", "RESULT", "CONFIDENCE", "LOCATION", "STUDY INFORMATION",
                                                            "IMAGING PARAMETER", "REGION", "Volume (mL)", "Region volume as percent of entire brain"),
                                             drop_cols = c("RESULT"),
                                             drop_values = list(c("null")), second_pass_drop = c(""),
                                             var_with_col_names = c("IMAGING RESULT/PARAMETER", "IMAGING PARAMETER", "REGION"),
                                             var_with_data = c("RESULT", "CONFIDENCE", "LOCATION", "Volume (mL)", "Region volume as percent of entire brain"),

                                             recode_key = "imaging_key",
                                             annotate_pet_cols = "PET", header_to_data = TRUE,

                                             reduction_vars = list(cols = c("STUDY INFORMATION"),
                                                                   gsub_match = c("^.*?; "), gsub_replace = c(""), collapse_string = "<br>",

                                                                   #Rexpand to multiple columns
                                                                   expand = (c(mim_t1_info = ".*?<br>(.*?T1.*?)<br>.*",
                                                                                   mim_t2_info = ".*?<br>(.*?T2.*?)<br>.*",
                                                                                   mim_swan_info = ".*?<br>(.*?SWAN.*?)<br>.*",
                                                                                   mim_pet_info = ".*?<br>(.*?PET.*?)<br>.*"))))
)


