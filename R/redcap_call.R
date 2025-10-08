#'
#' Some calls to REDcap to help match imaging dates to specific visits
#' As is often the case, we make a lot use of ADRCDash to handle the basics
#'



redcap_call <- function(rerun, .dict = redcap_dict){

  #Make most of the standard calls to the initial ADRCDash pipeline
  #Updated for the new ADRCDash

  #Read in the registry data
  regist_curr <- ADRCDash:::registry_read_in(synth = FALSE, use_spinner = FALSE)

  #Read in NACC visits; also includes biomarker inventory
  nacc_curr_list <-  ADRCDash:::visit_read_in(token = "REDCAP_NACC_API_NEW", subtable_dict = NULL, .type = "nacc", synth = FALSE)
  nacc_curr <- nacc_curr_list[["visits"]]
  rm(nacc_curr_list)

  #Read in Neuroimaging data
  neuroimage_curr_list <- ADRCDash:::visit_read_in(token = "REDCAP_NEUROIMAGE_API", .type = "neuroimage", synth = FALSE, all_cols = FALSE)
  neuroimage_curr <- neuroimage_curr_list[["visits"]]
  rm(neuroimage_curr_list)

  #Step wise merging 1, add registry to neuroimage, remap record / visit column names
  regist_curr[[.dict[["adrc_key"]]]] <- gsub("0(\\d{3})", "\\1", regist_curr[[.dict[["adrc_key"]]]])
  data_curr <- neuroimage_curr[regist_curr[regist_curr[[.dict[["adrc_key"]]]] %in% neuroimage_curr[[.dict[["adrc_key"]]]], ], on = eval(.dict[["adrc_key"]])]
  for(.idx in seq_along(.dict[["merge_remap"]][["image"]])){
    colnames(data_curr)[colnames(data_curr) == names(.dict[["merge_remap"]][["image"]])[.idx]] <- .dict[["merge_remap"]][["image"]][[.idx]]
  }

  #Step wise merging 2, add nacc to registry, remap record / vist column names
  uds_curr <- data_curr[nacc_curr[nacc_curr[[.dict[["adrc_key"]]]] %in% data_curr[[.dict[["adrc_key"]]]], ], on = eval(.dict[["adrc_key"]])]
  data_curr <- merge(data_curr, nacc_curr[nacc_curr[[.dict[["adrc_key"]]]] %in% data_curr[[.dict[["adrc_key"]]]], ], by = eval(.dict[["adrc_key"]]), all.x = TRUE)
  for(.idx in seq_along(.dict[["merge_remap"]][["nacc"]])){
    colnames(uds_curr)[colnames(uds_curr) == names(.dict[["merge_remap"]][["nacc"]])[.idx]] <- .dict[["merge_remap"]][["nacc"]][[.idx]]
    colnames(data_curr)[colnames(data_curr) == names(.dict[["merge_remap"]][["nacc"]])[.idx]] <- .dict[["merge_remap"]][["nacc"]][[.idx]]
  }

  #With two datasets (one P20 specific, the other neuroimaging specific) we process each separately
  data_out <- lapply(list(uds_curr, data_curr), function(.dat){

    #Now that we've done the initial pull on the full dataset, we can filter it down to get the columns we want
    #Nearly all of these are going to come from visit components
    .dat <- .dat[,colnames(.dat) %in% .dict[["retained"]], with = FALSE]
    .dat <- .dat[,order(match(colnames(.dat), .dict[["retained"]])), with = FALSE]

    #Also recast form_ver_num to have a V annotation
    .dat$form_ver_num <- gsub("^(?<!V)([0-9\\.]*)$", "V\\1", .dat$form_ver_num, perl = TRUE)

    ##
    #Notes 20251008 - TEMPORARY STOP GAP - WE WILL REMOVE THIS TO HAVE uds_form_map DIFFER BASED ON THE FORM ITSELF
    ##
    .dat$formver <- "V4"

    #Our other indexing step is to reduce the REDCap data set since we expect single neuroimaging visits spread across multiple visits
    #This is actually fairly straightforward as we can get minimums according to each imaging set
    #First get the minimums with a call to uds_image_match()
    .dat[, (.dict[["uds_visit_match"]][["image_match"]]) := uds_image_match(.SD), by = c(.dict[["adrc_key"]], .dict[["image_col"]]) ]

    #Finally, we just pull those rows that have at least one TRUE in the matching columns (be sure to remove NA's)
    .dat <- .dat[rowSums(.dat[,(.dict[["uds_visit_match"]][["image_match"]]),with=FALSE], na.rm = TRUE) > 0,]
    .dat <- as.data.frame(.dat)

    return(.dat)
  })

  #Name the list we're returning
  names(data_out) <- c("nacc", "imaging")

  #Return the processed data frame for subsequent matching
  return(data_out)
}


#The uds match data function used to minimize duplicate imaging rows
#Goal is to return a vector with a single TRUE corresponding to the UDS data most closely aligned to the imaging date
uds_image_match <- function(.dat, .dict = redcap_dict, .uds_col = "a1_form_dt"){

  #Step through the image date columns to compare against the backup, coerce both dates to columns and take the difference in days (absolute value)
  uds_image_diff <- lapply(.dict[["uds_visit_match"]][["image_cols"]], function(.col){
    abs(as.Date(.dat[[.col]]) - as.Date(.dat[[.uds_col]]))
  })

  #Now we check each column (again, in case both Abeta and Tau exist) to see which row is the minimum
  uds_image_diff <- lapply(uds_image_diff, function(.col){.col == min(.col)})

  #Make it a data frame
  uds_image_diff <- do.call(cbind.data.frame, uds_image_diff)
  colnames(uds_image_diff) <- .dict[["uds_visit_match"]][["image_match"]]

  #If there isn't a NACC associated visit we may still want to pull the visit, in this case we just return the first row
  #There should only ever be one row in this case but we can be careful
  if(sum(!is.na(uds_image_diff)) == 0){
    uds_image_diff <- do.call(cbind.data.frame, rep(list(c(TRUE, rep(FALSE, (nrow(.dat)-1) ))), length(.dict[["uds_visit_match"]][["image_cols"]])))
    colnames(uds_image_diff) <- .dict[["uds_visit_match"]][["image_match"]]
  }

  #With the minimum rows identified we can return the boolean data frame
  return(uds_image_diff)
}




# #The old version using phase 2 REDCap
#
# redcap_call_old <- function(rerun)
#
#
#   #Read in the data - note we keep the record IDs for uploading to REDCap later
#   data_curr <- ADRCDash:::redcap_read_in(simple = TRUE, synth = FALSE, use_spinner = FALSE,
#                                          dropped_cols_nacc = c("redcap_survey_identifier", "adni_guid", "ppmp_guid", "adc_cntr_id"))
#
#   #We take the record_id from the NACC project and recast it as $record_id for use later
#   data_curr$record_id <- data_curr$record_id.y
#   data_curr <- data_curr[!is.na(data_curr$record_id),]
#
#   #Drop test cases
#   data_curr <- ADRCDash:::redcap_filter_test_cases(data_curr, use_spinner = FALSE)
#   #Drop the clinical brain donors or other non-valid studies
#   data_curr <- ADRCDash:::redcap_filter_specific_studies(data_curr)
#   #Drop the invalid rows missing an A1 date or a prescreen date
#   data_curr <- ADRCDash:::redcap_drop_invalid_rows(data_curr)
#   #Make sure the dataframe is properly sorted according to date
#   data_curr <- ADRCDash:::redcap_order_rows(data_curr)
#   #Fill down the pertinent rows as needed
#   data_curr <- ADRCDash:::fill_down_rows(data_curr)
#
#   #Now that we've done the initial pull on the full dataset, we can filter it down to get the columns we want
#   #Nearly all of these are going to come from visit components
#   data_curr <- data_curr[,colnames(data_curr) %in% redcap_dict[["retained"]]]
#   data_curr <- data_curr[,order(match(colnames(data_curr), redcap_dict[["retained"]]))]
#
#   #Just for ease of use we also drop rows that don't have a valid form_ver_num (it's the easiest thing to filter on right now)
#   data_curr <- data_curr[!is.na(data_curr$form_ver_num),]
#   #Also recast form_ver_num 3.1 to V3.1
#   data_curr$form_ver_num <- gsub("^(3\\.1)$", "V\\1", data_curr$form_ver_num)
#
#   #Return the processed data frame for subsequent matching
#   return(data_curr)
# }





#'
#'
#' The various dictionaries we use that are specific to REDCap
#'
#'


#This dictionary helps identify the columns we retain to build the dataframe (retained)
#...and the columns used for the generalized imaging instrument
#Works on both the neuroimaging and nacc visit calls since they have to be pulled separately
#We just make use of the uds_questions dictionary when compiling the D1 specific uploads

redcap_dict = list(retained = c("nacc_record", "image_record", "adc_sub_id",
                                "nacc_event", "image_event",
                                "nacc_visit", "image_visit",
                                "form_ver_num", "abeta_visit_dt", "tau_visit_dt", "a1_form_dt"),

                   imaging_inst_exported = c(),

                   adrc_key = "adc_sub_id", redcap_key = "record_id",
                   visit_col = "redcap_repeat_instance", image_col = "image_visit",

                   merge_remap = list(nacc = c(record_id = "nacc_record", redcap_repeat_instance = "nacc_visit", redcap_event_name = "nacc_event"),
                                      image = c(record_id = "image_record", redcap_repeat_instance = "image_visit", redcap_event_name = "image_event")),

                   uds_visit_match = list(image_cols = c("abeta_visit_dt", "tau_visit_dt"),
                                          image_match = c("abeta_match", "tau_match")))



#List associating MIM question to UDS question and REDCap header
#  quest_num is the starting string of column names i.e. what we extract from the mim tables as "variable"
#  quest_id is the REDCap specific names that we map to
#  default_response is the stock answer we want to send to REDCap in order to fill in missing columns for NACC purposses mainly
#    This can either be a single length character (e.g. "" for the Imaging mapping dictionary) or a vector as long as quest_num / quest_id
#  uds_recode and uds_null are used for any last minute recodings, again mainly for NACC upload purposes
#    This helps us get from the mim null responses to the NACC unknowns we want in REDCap
#  uds_ver_col identifies which column to reference in order to subset any data, the name is the column in REDCap while the entry is the column in the MIM data
#    If it's NA it assumes no subsetting is done and applies it to the entire dataset (see Imaging dictionary for an example)
#  reduc_collapse_string is used for columns that get reduced
#    It has no bearing for this dictionary but is important for things like STUDY INFORMATION in the Imaging dictionary to avoid mismatches in merge_nacc_rows

# uds_questions <-
#   list(
#     UDS4 =
#       list(
#         # Updated NACC item codes for new forms
#         quest_num = c(
#           # PET section
#           "6a1", "6a2", "6b", "6b1", "6b2", "6b3", "64b", "6d",
#           # MRI/PET details carried forward to keep merge_nacc_rows behavior coherent
#           "7a1", "7a2", "7a3", "7a3a", "7a3b", "7a3c", "7a3d", "7a3e", "7a3e1",
#           # Legacy fields still present in new tables
#           "6e", "6h"
#         ),
#         quest_id = c(
#           # Map to REDCap fields; adapt as available in your REDCap project
#           "amylpet", "taupetad", "fdgad", "fdgad_ad", "fdgftld", "fdgdlb", "fdgothx", "hippatr",
#           "mr_ad", "mr_ftld", "mr_cvd", "imaglinf", "imaglac", "imagmach", "imagmich", "imagmwmh", "imagewmh",
#           # Backward-compatible fields used elsewhere
#           "taupetad", "tpetftld"
#         ),
#         default_response = rep(8, 20),
#         uds_recode = c(8),
#         uds_null = c("null"),
#         uds_ver_col = c("form_ver_num" = "uds_version"),
#         reduce_collapse_string = NA
#       )
#   )

#
# uds_form_map <- list(redcap_col = "form_ver_num",
#                      map = data.frame(V4 = "UDS4"))

uds_questions <-
  list(UDS3 =
         list(
           # Updated NACC item codes for new forms
           quest_num = c(
             # PET section
             "6a", "6b", "6c", "6d", "6e", "6f", "6g", "6h", "6i", "6j",
             # Original MRI/PET
             "7a", "7b", "7c", "7d", "7e", "7f",
             # Genetics for UDS3
             "8", "9", "10", "10a"
           ),
           quest_id = c(
             # Map to REDCap fields; adapt as available in your REDCap project
             "amylpet", "amylcsf", "fdgad", "hippatr", "taupetad", "csftau", "fdgftld", "tpetftld", "mrftld", "datscan", "othbiom", "othbiomx",
             "imaglinf", "imaglac", "imagmach", "imagmich", "imagmwmh", "imagewmh",
             "admut", "ftldmut", "othmut", "othmutx"
           ),

              default_response = c(rep(8,10), 0, "", rep(8,9), ""),
              uds_recode = c(8),
              uds_null = c("null"),
              uds_ver_col = c("form_ver_num" = "uds_version"),
              reduce_collapse_string = NA
         ),

       UDS4_to_UDS3 =
         list(
             # Updated NACC item codes for new forms
           quest_num = c(
             # PET section
             "6a1", "6a2", "6b", "6b1", "6b2", "6b3", "64b", "64b", "6d",
             # MRI/PET details carried forward to keep merge_nacc_rows behavior coherent
             "7a1", "7a2", "7a3", "7a3a", "7a3b", "7a3c", "7a3d", "7a3e", "7a3e1",
             # UDS3 Genetics no longer used in 4
             "8", "9", "10", "10a"
           ),
           quest_id = c(
             # Map to REDCap fields; adapt as available in your REDCap project
             "amylpet", "taupetad", "fdgpetdx", "fdgad", "fdgftld", "fdglbd", "fdgoth", "fdgothx", "hippatr",
             "mr_ad", "mr_ftld", "mr_cvd", "imaglinf", "imaglac", "imagmach", "imagmich", "imagmwmh", "imagewmh"
           ),

             default_response = c(rep(8,6), 0, "", rep(8, 10)),
             uds_recode = c(8),
             uds_null = c("null"),
            uds_ver_col = c("form_ver_num" = "uds_version"),
            reduce_collapse_string = NA

         ),

       ##
       #To adjust 20251008 - We've identified a few questions Jon will need to adjust, specifically 6b4 and 6d_old, see below for where they are entered
       ##

       UDS4 =
         list(
           # Updated NACC item codes for new forms
           quest_num = c(
             "5",
             # PET section
             "6a", "6a1", "6a2", "6b", "6b1", "6b2", "6b3", "64b", "6b4a",    #"6b4", "6b4a",   #Replace this when Jon no longer has 64b
             "6c", "6d", "6d1", "6d2", "6d3", "6d4", "6d4a",
             #Special hippocampal atrophy variable, probably drop for UDS4
             "6d", # "6d_old",  #Again, replace this once Jon annotates hippocampal atrophy as "6d_old"
             # MRI details carried forward to keep merge_nacc_rows behavior coherent
             "7a", "7a1", "7a2", "7a3", "7a3a", "7a3b", "7a3c", "7a3d", "7a3e", "7a3e1"
           ),
           quest_id = c(
             "imagindx",
             # Map to REDCap fields; adapt as available in your REDCap project
             "petdx", "amylpet", "taupet", "fdgpetdx", "fdgad", "fdgftld", "fdglbd", "fdgoth", "fdgothx",
             "datscandx", "tracothdx", "tracerad", "tracerftld", "tracerlbd", "traceroth", "tracerothx",
             "hippatr",
             # MRI column names
             "structdx", "structad", "structftld", "structcvd", "imaglinf", "imaglac", "imagmach", "imagmich", "imagmwmh", "imagwmhsev"
           ),
              default_response = c(0, 0, rep(8,2), 0, rep(8,4), "", 0, 0, "", rep(8,4), "",
                                   8,
                                   0, rep(8,3), rep(8,5), ""),
              uds_recode = c(8),
              uds_null = c("null"),
              uds_ver_col = c("form_ver_num" = "uds_version"),
              reduce_collapse_string = NA
         )
  )

##
#Notes 20251008 - We will need to coerce uds_form_map to have both a UDS3 and UDS4 entry since the form version variable is form_ver_num in UDS3 and formver in UDS4
##

uds_form_map <- list(redcap_col = "form_ver_num",
                     map = data.frame(V3.1 = "UDS3", V4 = "UDS4")
)

uab_imaging_map <- list(redcap_col = NA,
                        map = data.frame(Imaging = "Imaging")
                        )


#The remapping of UDS values
uds_key <- c("Yes"=1, "No"=0, "Unknown"=8, "null"="null", "Cannot assess"=8)

#The remapping of Imaging values
imaging_key <- c("Not applicable"="", "null"="", "Cannot assess"=8)



#Redcap field names
redcap_id <- "adc_sub_id"
redcap_event <- "redcap_event_name"

