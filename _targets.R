library(targets)
library(tarchetypes)

# Set target-specific options such as packages.
tar_option_set(packages = c("ADRCDash", "naccDataDict"))
#tar_option_set(packages = c("ADRCDash"))


#Source /R
R.utils::sourceDirectory("./R/", modifiedOnly = FALSE)

targets::tar_destroy("local")

# Define targets
targets <- list(

  #Pre-processing - read in all files in the input directory
  tar_target(
    input_dir,
    paste0("./input/", list.files("./input/")),
    format = "file"
  ),

  #Pre-processing - build out the list of read-in data
  tar_target(
    data_list,
    parse_csv(input_dir)
  ),

  #Now data_list has one entry for each file that's been read in from the directory
  #Within each entry is a set of header information and meta data ($header)
  #There's another list called $tables which has three processed tables that can then be recast into a REDCap format

  #REDCap uploads - starting dataframe

  #First make a call to REDCap to get prospective dates, we can do this using our usual ADRCDash and naccDataDict calls
  tar_target(
    sys_curr,
    Sys.time()
  ),
  tar_target(
    redcap_match,
    redcap_call(rerun = sys_curr)
  ),
  #This creates a starting dataframe we can use to populate for the D1 and the imaging instrument



  #Prep the D1 data for REDCap upload

  #First reduce the list into a dataframe of known REDCap questions
  tar_target(
    nacc_d1_dat,
    process_csv(data_list, entry_form = "D1")
  ),

  #nacc_d1_dat is now a dataframe with each row corresponding to a mim file that's comprised solely NACC questions
  #The next step is to merge PiB and AV1451 rows together based on adc_sub_id and Image_visit
  #nacc_d1_merge has essentially replaced what was previously done by in csv_to_d1_uds()
  #We've added the force_latest argument as well to account for mismatches (default is to print a warning and return NULL)
  tar_target(
    nacc_d1_merge,
    merge_nacc_rows(nacc_d1_dat, mim_dict = uds_questions, force_latest = TRUE)
  ),

  #We also process by individual scans just in case we need have images split over multiple visits
  tar_target(
    nacc_d1_single_row,
    merge_nacc_rows(nacc_d1_dat, mim_dict = uds_questions, by_vector = c("adc_sub_id", "PET"))
  ),


  #The final step is to match the MIM data to the REDCap pull so it's ready for upload
  #This is a generalized step that can be applied for either NACC data or UAB specific instruments
  tar_target(
    uds_output,
    merge_to_csv(nacc_d1_merge, redcap_match[["nacc"]], csv_string = "redcap_D1_input_", .type = "nacc",
                 mim_dict = uds_questions, form_map = uds_form_map,
                 .mim_backup = nacc_d1_single_row$UDS4)
  )



  # #Also prep the full dataset to the upload for to the generalized imaging instrument
  # tar_target(
  #   imaging_dat,
  #   process_csv(data_list, entry_form = "Imaging")
  # ),
  #
  # #Do the same type of merge and CSV output
  # tar_target(
  #   imaging_merge,
  #   merge_nacc_rows(imaging_dat, mim_dict = mim_to_redcap_dict)
  # ),
  #
  # tar_target(
  #   imaging_output,
  #   merge_to_csv(imaging_merge, redcap_match[["imaging"]], csv_string = "redcap_mim_input_", .type = "image",
  #                mim_dict = mim_to_redcap_dict, form_map = uab_imaging_map)
  # )

)


targets
