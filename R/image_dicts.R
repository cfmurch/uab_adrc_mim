#'
#'
#' Dictionaries and functions used by image processing functions
#'
#'



#A general purpose function to split on :: to call a function from a package using the getExportedValue function
getfunc<-function(x) {
  if(length(grep("::", x))>0) {
    parts<-strsplit(x, "::")[[1]]
    getExportedValue(parts[1], parts[2])
  } else {
    x
  }
}

`%not_in%` <- Negate(`%in%`)






#Dictionary for parsing the file name and header
#Uses column names the the gsub extraction string (assumes any dataframe collapses using "|")
#Lubridate calls are included and if !is.na() will use get() to coerce the string to a Date class

parsing_dict <- list(UDS3 =
                       list(file_name =
                              list(col_names = c("adc_sub_id", "PET", "Image_date"),
                                   gsub_string = c(".*?(ADC\\d*).*", ".*?(AV(\\d)+|PiB|Tau|Abeta).*", ".*?_(\\d*)_.*"),
                                   date_string = c(NA, NA, "lubridate::ymd")),
                            header =
                              list(col_names = c("adc_sub_id", "mim_neuro_id", "Image_visit", "uds_version", "MIM_date", "Analyzed_by",
                                                 "Image_date", "Tau_date", "Amyloid_date", "MRI_date"),
                                   gsub_string = c(".*?MRN: (ADC\\d*)_.*", ".*?Subject ID: (.*?)_.*", ".*?Subject ID: .*?((x|X)\\d*)_.*", ".*?(UDS\\d*)_.*",
                                                   ".*?Date analysis performed: (\\d*)_.*",
                                                   ".*?Analysis performed by: (.*?)_.*", ".*?Date of acquisition: (.*?)_.*",
                                                   ".*?Date of tau.*?: (.*?)_.*", ".*?Date of amyloid.*?: (.*?)_.*", ".*?Date of MRI.*?: (.*?)_.*"),
                                                   #"null.*?PET BRAIN MAC.*? \\- (.*?)_.*?null.*", ".*?PET BRAIN MAC.*? \\- (.*?)_.*?null.*?_.*?null.*", ".*?Date of MRI.*? \\- (.*?)"),
                                   date_string = c(NA, NA, NA, NA, "lubridate::ymd", NA, "lubridate::ymd", "lubridate::ymd", "lubridate::ymd", "lubridate::ymd")),

                            tail =
                              list(row_ident = "^(Amyloid SUVR|Tauopathy summary score|Centiloid value \\(CL\\)|SUVR for CL|Centiloid equation)",
                                   col_names = c("amyl_suvr", "tau_score", "amyl_centiloid", "amyl_suvr_cl", "amyl_centiloid_eq"),
                                   gsub_string = c(".*?cortical SUVR for amyloid: (.*?)_.*", ".*?Taupathy summary score: (.*?)_.*",
                                                   "Centiloid value \\(CL\\)_(.*?)_.*", ".*?SUVR for CL calculation_(\\d+\\.\\d+)_.*", ".*? x SUVR\\) \\- (.*?)_.*"),
                                   date_string = c(NA, NA, NA, NA, NA)),

                            annotate = c("mim_neuro_id", "MIM_date", "Analyzed_by", "MRI_date")),

                     UDS4_to_UDS3 =
                       list(file_name =
                              list(col_names = c("adc_sub_id", "PET", "Image_date"),
                                   gsub_string = c(".*?(ADC\\d*).*", ".*?(AV(\\d)+|PiB|Tau|Abeta).*", ".*?_(\\d*)_.*"),
                                   date_string = c(NA, NA, "lubridate::ymd")),
                            header =
                              list(col_names = c("adc_sub_id", "mim_neuro_id", "Image_visit", "uds_version", "MIM_date", "Analyzed_by",
                                                 "Image_date", "Tau_date", "Amyloid_date", "MRI_date"),
                                   gsub_string = c(".*?MRN: (ADC\\d*)_.*", ".*?Subject ID: (.*?)_.*", ".*?Subject ID: .*?((x|X)\\d*)_.*", ".*?(UDS\\d*)_.*",
                                                   ".*?Date analysis performed: (\\d*)_.*",
                                                   ".*?Analysis performed by: (.*?)_.*", ".*?Date of acquisition: (.*?)_.*",
                                                   #".*?Date of tau.*?: (.*?)_.*", ".*?Date of amyloid.*?: (.*?)_.*", ".*?Date of MRI.*?: (.*?)_.*"),
                                                   "null.*?PET BRAIN MAC.*? \\- (.*?)_.*?null.*", ".*?PET BRAIN MAC.*? \\- (.*?)_.*?null.*?_.*?null.*", ".*?Date of MRI.*? \\- (.*?)"),
                                   date_string = c(NA, NA, NA, NA, "lubridate::ymd", NA, "lubridate::ymd", "lubridate::ymd", "lubridate::ymd", "lubridate::ymd")),

                            tail =
                              list(row_ident = "^(Amyloid SUVR|Tauopathy summary score|Centiloid value \\(CL\\)|SUVR for CL|Centiloid equation)",
                                   col_names = c("amyl_suvr", "tau_score", "amyl_centiloid", "amyl_suvr_cl", "amyl_centiloid_eq"),
                                   gsub_string = c(".*?cortical SUVR for amyloid: (.*?)_.*", ".*?Taupathy summary score: (.*?)_.*",
                                                   "Centiloid value \\(CL\\)_(.*?)_.*", ".*?SUVR for CL calculation_(\\d+\\.\\d+)_.*", ".*? x SUVR\\) \\- (.*?)_.*"),
                                   date_string = c(NA, NA, NA, NA, NA)),

                            annotate = c("mim_neuro_id", "MIM_date", "Analyzed_by", "MRI_date")),

                     UDS4 =
                       list(file_name =
                              list(col_names = c("adc_sub_id", "PET", "Image_date"),
                                   gsub_string = c(".*?(ADC\\d*).*", ".*?(AV(\\d)+|PiB|Tau|Abeta).*", ".*?_(\\d*)_.*"),
                                   date_string = c(NA, NA, "lubridate::ymd")),
                            header =
                              list(col_names = c("adc_sub_id", "mim_neuro_id", "Image_visit", "uds_version", "MIM_date", "Analyzed_by",
                                                 "Image_date", "Tau_date", "Amyloid_date", "MRI_date"),
                                   gsub_string = c(".*?MRN: (ADC\\d*)_.*", ".*?Subject ID: (.*?)_.*", ".*?Subject ID: .*?((x|X)\\d*)_.*", ".*?(UDS\\d*)_.*",
                                                   ".*?Date analysis performed: (\\d*)_.*",
                                                   ".*?Analysis performed by: (.*?)_.*", ".*?Date of acquisition: (.*?)_.*",
                                                   #".*?Date of tau.*?: (.*?)_.*", ".*?Date of amyloid.*?: (.*?)_.*", ".*?Date of MRI.*?: (.*?)_.*"),
                                                   "null.*?PET BRAIN MAC.*? \\- (.*?)_.*?null.*", ".*?PET BRAIN MAC.*? \\- (.*?)_.*?null.*?_.*?null.*", ".*?Date of MRI.*? \\- (.*?)"),
                                   date_string = c(NA, NA, NA, NA, "lubridate::ymd", NA, "lubridate::ymd", "lubridate::ymd", "lubridate::ymd", "lubridate::ymd")),

                            tail =
                              list(row_ident = "^(Amyloid SUVR|Tauopathy summary score|Centiloid value \\(CL\\)|SUVR for CL|Centiloid equation)",
                                   col_names = c("amyl_suvr", "tau_score", "amyl_centiloid", "amyl_suvr_cl", "amyl_centiloid_eq"),
                                   gsub_string = c(".*?cortical SUVR for amyloid: (.*?)_.*", ".*?Taupathy summary score: (.*?)_.*",
                                                   "Centiloid value \\(CL\\)_(.*?)_.*", ".*?SUVR for CL calculation_(\\d+\\.\\d+)_.*", ".*? x SUVR\\) \\- (.*?)_.*"),
                                   date_string = c(NA, NA, NA, NA, NA)),

                            annotate = c("mim_neuro_id", "MIM_date", "Analyzed_by", "MRI_date")))












#A variety of information related to the CSV parsing
# key is the possible table names for PET data, Vascular data, Structural Volumes
csv_list_dict <- list(UDS3 =
                        list(key = c("PET", "Vascular", "Volumes"),
                             PET_col = c("IMAGING RESULT/PARAMETER", "RESULT", "NACC ITEM ID", "CONFIDENCE", "STUDY INFORMATION"),
                             Vascular_col = c("IMAGING PARAMETER", "RESULT", "NACC ITEM ID", "LOCATION", "STUDY INFORMATION"),
                             Volumes_col = c("REGION", "Volume (mL)", "Region volume as percent of entire brain")),
                      UDS4_to_UDS3 = list(key = c("PET", "Atrophy", "Vascular", "Volumes"),
                                          PET_col = c("IMAGING RESULT/PARAMETER", "RESULT", "NACC ITEM ID", "CONFIDENCE", "STUDY INFORMATION"),
                                          Atrophy_col = c("IMAGING RESULT/PARAMETER", "", "NACC", "CONFIDENCE", "STUDY INFORMATION"),
                                          Vascular_col = c("IMAGING PARAMETER", "RESULT", "NACC ITEM ID", "LOCATION", "STUDY INFORMATION"),
                                          Volumes_col = c("REGION", "Volume (mL)", "Region volume as percent of entire brain")),
                      UDS4 = list(key = c("PET", "Atrophy", "Vascular", "Volumes"),
                                  PET_col = c("IMAGING RESULT/PARAMETER", "RESULT", "NACC ITEM ID", "CONFIDENCE", "STUDY INFORMATION"),
                                  Atrophy_col = c("IMAGING RESULT/PARAMETER", "", "NACC", "CONFIDENCE", "STUDY INFORMATION"),
                                  Vascular_col = c("IMAGING PARAMETER", "RESULT", "NACC ITEM ID", "LOCATION", "STUDY INFORMATION"),
                                  Volumes_col = c("REGION", "Volume (mL)", "Region volume as percent of entire brain")))




#Matching neuroimaging date columns to specific list entries
#We allow for multiple matching within mim since some of these column names get remapped (Imaging) for import while other (UDS3 and UDS4) don't get uploaded
date_col_match <- list(AV1451 = c(Tau_date = "tau_visit_dt",
                                  tau_mim_dt = "tau_visit_dt"),
                       PIB = c(Amyloid_date = "abeta_visit_dt",
                               abeta_mim_dt = "abeta_visit_dt"),
                       All = c(Tau_date = "tau_visit_dt", Amyloid_date = "abeta_visit_dt",
                               tau_mim_dt = "tau_visit_dt", abeta_mim_dt = "abeta_visit_dt"))












#Our Imaging to redcap dictionary, similar in structure to the original UDS questions but much, much longer
#See the uds_questions dicitonary in redcal_call.R for details on the entries

mim_to_redcap_dict <-
  list(Imaging = list(
    quest_num = c(#We initialize on the header and meta data we pull in,
                  "Tau_date", "Amyloid_date", "MIM_date_tau", "MIM_date_abeta", "amyl_suvr", "tau_score", "mim_neuro_id_tau", "mim_neuro_id_abeta", "Analyzed_by_tau", "Analyzed_by_abeta",
                  "mim_t1_info_tau", "mim_t2_info_tau", "mim_swan_info_tau", "mim_pet_info_tau", "mim_t1_info_abeta", "mim_t2_info_abeta", "mim_swan_info_abeta", "mim_pet_info_abeta",

                  #The first set is the amyloid specific questions from the PET table (question 6 from NACC) followed by the tau questions
                  "Abnormally elevated amyloid on PET?_PIB_RESULT", "Abnormally elevated amyloid on PET?_PIB_CONFIDENCE", "Hippocampal atrophy (Duara 2-4 on right and/or left)?_PIB_RESULT", "Right hippocampal atrophy (Duara 2-4)?_PIB_RESULT", "Right hippocampal atrophy Duara score (0-4)_PIB_RESULT", "Left hippocampal atrophy (Duara 2-4)?_PIB_RESULT", "Left hippocampal atrophy Duara score (0-4)_PIB_RESULT", "Structural MRI evidence of FTLD?_PIB_RESULT", "Structural MRI evidence of FTLD?_PIB_CONFIDENCE",
                  "Hippocampal atrophy (Duara 2-4 on right and/or left)?_AV1451_RESULT", "Right hippocampal atrophy (Duara 2-4)?_AV1451_RESULT", "Right hippocampal atrophy Duara score (0-4)_AV1451_RESULT", "Left hippocampal atrophy (Duara 2-4)?_AV1451_RESULT", "Left hippocampal atrophy Duara score (0-4)_AV1451_RESULT", "Tau-PET evidence for AD?_AV1451_RESULT", "Tau-PET evidence for AD?_AV1451_CONFIDENCE", "Tau-PET evidence of FTLD?_AV1451_RESULT", "Tau-PET evidence of FTLD?_AV1451_CONFIDENCE", "Structural MRI evidence of FTLD?_AV1451_RESULT", "Structural MRI evidence of FTLD?_AV1451_CONFIDENCE",

                  #As an aside, this would be a good part to interleave any FDG or DAT scan data which is part of question 6
                  #We currently don't have any scans that do this but again, this has been built with scalability in mind

                  #Next is the vascular based table which is basically all MRI so we again have both a amyloid and tau specific set
                  "Large vessel infarct(s)?_PIB_RESULT", "Large vessel infarct(s)?_PIB_LOCATION", "Lacunar infarct(s)?_PIB_RESULT", "Lacunar infarct(s)?_PIB_LOCATION", "Macrohemorrhage(s)?_PIB_RESULT", "Macrohemorrhage(s)?_PIB_LOCATION", "Microhemorrage(s)?_PIB_RESULT", "Microhemorrage(s)?_PIB_LOCATION", "Moderate white matter hyperintensities?(CHS 5-6)_PIB_RESULT", "Extensive white matter hyperintensities?(CHS 7 or 8)_PIB_RESULT", "CHS score (1-8)_PIB_RESULT", "Fazekas score for periventricular white matter_PIB_RESULT", "Fazekas score for deep white matter_PIB_RESULT",
                  "Large vessel infarct(s)?_AV1451_RESULT", "Large vessel infarct(s)?_AV1451_LOCATION", "Lacunar infarct(s)?_AV1451_RESULT", "Lacunar infarct(s)?_AV1451_LOCATION", "Macrohemorrhage(s)?_AV1451_RESULT", "Macrohemorrhage(s)?_AV1451_LOCATION", "Microhemorrage(s)?_AV1451_RESULT", "Microhemorrage(s)?_AV1451_LOCATION", "Moderate white matter hyperintensities?(CHS 5-6)_AV1451_RESULT", "Extensive white matter hyperintensities?(CHS 7 or 8)_AV1451_RESULT", "CHS score (1-8)_AV1451_RESULT", "Fazekas score for periventricular white matter_AV1451_RESULT", "Fazekas score for deep white matter_AV1451_RESULT",

                  #The minimal set currently in REDCap
                  "Right hippocampal atrophy Duara score (0-4)_AV1451_RESULT", "Left hippocampal atrophy Duara score (0-4)_AV1451_RESULT", "Fazekas score for periventricular white matter_AV1451_RESULT", "Fazekas score for deep white matter_AV1451_RESULT",
                  "Right hippocampal atrophy Duara score (0-4)_PIB_RESULT", "Left hippocampal atrophy Duara score (0-4)_PIB_RESULT", "Fazekas score for periventricular white matter_PIB_RESULT", "Fazekas score for deep white matter_PIB_RESULT",


                  #Finally, there are the brain volumes, again all MRI so we have two series, defined according to volume and percent
                  "Entire brain_PIB_Volume (mL)", "Entire cerebellum_PIB_Volume (mL)", "Entire cerebellum_PIB_Region volume as percent of entire brain", "Entire cerebral cortex_PIB_Volume (mL)", "Entire cerebral cortex_PIB_Region volume as percent of entire brain", "Left caudate_PIB_Volume (mL)", "Left caudate_PIB_Region volume as percent of entire brain", "Right caudate_PIB_Volume (mL)", "Right caudate_PIB_Region volume as percent of entire brain", "Left frontal cortex_PIB_Volume (mL)", "Left frontal cortex_PIB_Region volume as percent of entire brain", "Right frontal cortex_PIB_Volume (mL)", "Right frontal cortex_PIB_Region volume as percent of entire brain",
                  "Left hippocampus_PIB_Volume (mL)", "Left hippocampus_PIB_Region volume as percent of entire brain", "Right hippocampus_PIB_Volume (mL)", "Right hippocampus_PIB_Region volume as percent of entire brain", "Left lateral ventricle_PIB_Volume (mL)", "Left lateral ventricle_PIB_Region volume as percent of entire brain", "Right lateral ventricle_PIB_Volume (mL)", "Right lateral ventricle_PIB_Region volume as percent of entire brain", "Left occipital cortex_PIB_Volume (mL)", "Left occipital cortex_PIB_Region volume as percent of entire brain", "Right occipital cortex_PIB_Volume (mL)", "Right occipital cortex_PIB_Region volume as percent of entire brain",
                  "Left parietal cortex_PIB_Volume (mL)", "Left parietal cortex_PIB_Region volume as percent of entire brain", "Right parietal cortex_PIB_Volume (mL)", "Right parietal cortex_PIB_Region volume as percent of entire brain", "Left putamen_PIB_Volume (mL)", "Left putamen_PIB_Region volume as percent of entire brain", "Right putamen_PIB_Volume (mL)", "Right putamen_PIB_Region volume as percent of entire brain", "Left temporal cortex_PIB_Volume (mL)", "Left temporal cortex_PIB_Region volume as percent of entire brain", "Right temporal cortex_PIB_Volume (mL)", "Right temporal cortex_PIB_Region volume as percent of entire brain",

                  "Entire brain_AV1451_Volume (mL)", "Entire cerebellum_AV1451_Volume (mL)", "Entire cerebellum_AV1451_Region volume as percent of entire brain", "Entire cerebral cortex_AV1451_Volume (mL)", "Entire cerebral cortex_AV1451_Region volume as percent of entire brain", "Left caudate_AV1451_Volume (mL)", "Left caudate_AV1451_Region volume as percent of entire brain", "Right caudate_AV1451_Volume (mL)", "Right caudate_AV1451_Region volume as percent of entire brain", "Left frontal cortex_AV1451_Volume (mL)", "Left frontal cortex_AV1451_Region volume as percent of entire brain", "Right frontal cortex_AV1451_Volume (mL)", "Right frontal cortex_AV1451_Region volume as percent of entire brain",
                  "Left hippocampus_AV1451_Volume (mL)", "Left hippocampus_AV1451_Region volume as percent of entire brain", "Right hippocampus_AV1451_Volume (mL)", "Right hippocampus_AV1451_Region volume as percent of entire brain", "Left lateral ventricle_AV1451_Volume (mL)", "Left lateral ventricle_AV1451_Region volume as percent of entire brain", "Right lateral ventricle_AV1451_Volume (mL)", "Right lateral ventricle_AV1451_Region volume as percent of entire brain", "Left occipital cortex_AV1451_Volume (mL)", "Left occipital cortex_AV1451_Region volume as percent of entire brain", "Right occipital cortex_AV1451_Volume (mL)", "Right occipital cortex_AV1451_Region volume as percent of entire brain",
                  "Left parietal cortex_AV1451_Volume (mL)", "Left parietal cortex_AV1451_Region volume as percent of entire brain", "Right parietal cortex_AV1451_Volume (mL)", "Right parietal cortex_AV1451_Region volume as percent of entire brain", "Left putamen_AV1451_Volume (mL)", "Left putamen_AV1451_Region volume as percent of entire brain", "Right putamen_AV1451_Volume (mL)", "Right putamen_AV1451_Region volume as percent of entire brain", "Left temporal cortex_AV1451_Volume (mL)", "Left temporal cortex_AV1451_Region volume as percent of entire brain", "Right temporal cortex_AV1451_Volume (mL)", "Right temporal cortex_AV1451_Region volume as percent of entire brain"
    ),


    quest_id = c(#Again, we start with header and meta data by REDCap column ID
                 "tau_mim_dt", "abeta_mim_dt", "tau_mim_dt", "abeta_mim_dt", "abeta_mim_suvr", "tau_mim_tauopathy", "tau_mim_id", "abeta_mim_id", "tau_mim_analyst", "abeta_mim_analyst",
                 "tau_mim_t1_info", "tau_mim_t2_info", "tau_mim_swan_info", "tau_mim_pet_info", "abeta_mim_t1_info", "abeta_mim_t2_info", "abeta_mim_swan_info", "abeta_mim_pet_info",


                 #The amyloid data from the PET table to start follow by the tau
                 "mim_elev_amyl_res_amyl", "mim_elev_amyl_conf_amyl", "mim_hippoc_atrophy_res_amyl", "mim_right_hippoc_atrophy_res_amyl", "mim_right_hippoc_duara_res_amyl", "mim_left_hippoc_atrophy_res_amyl", "mim_left_hippoc_duara_res_amyl", "mim_mri_ftld_res_amyl", "mim_mri_ftld_conf_amyl",
                 "mim_hippoc_atrophy_res_tau", "mim_right_hippoc_atrophy_res_tau", "mim_right_hippoc_duara_res_tau", "mim_left_hippoc_atrophy_res_tau", "mim_left_hippoc_duara_res_tau", "mim_tau_pet_ad_res_tau", "mim_tau_pet_ad_conf_tau", "mim_fdg_pet_fltd_res_tau", "mim_fdg_pet_fltd_conf_tau", "mim_tau_pet_ftld_res_tau", "mim_tau_pet_ftld_conf_tau",

                 #As mentioned above, a good spot to input things like DAT or FDG scans if they are ever done

                 #Here are the vasuclar results for amyloid and tau
                 "mim_vessel_infarct_res_amyl", "mim_vessel_infarct_locat_amyl", "mim_lacuna_infarct_res_amyl", "mim_lacuna_infarct_locat_amyl", "mim_macro_hemor_res_amyl", "mim_macro_hemor_locat_amyl", "mim_micro_hemor_res_amyl", "mim_micro_hemor_locat_amyl", "mim_moderate_wmh_res_amyl", "mim_extensive_wmh_res_amyl", "mim_chs_score_res_amyl", "mim_fazekas_peri_res_amyl", "mim_fazekas_deep_res_amyl",
                 "mim_vessel_infarct_res_tau", "mim_vessel_infarct_locat_tau", "mim_lacuna_infarct_res_tau", "mim_lacuna_infarct_locat_tau", "mim_macro_hemor_res_tau", "mim_macro_hemor_locat_tau", "mim_micro_hemor_res_tau", "mim_micro_hemor_locat_tau", "mim_moderate_wmh_res_tau", "mim_extensive_wmh_res_tau", "mim_chs_score_res_tau", "mim_fazekas_peri_res_tau", "mim_fazekas_deep_res_tau",

                 #This is the minimal set currently in REDCap
                 "tau_mim_duara_r", "tau_mim_duara_l", "tau_mim_fazekas_peri", "tau_mim_fazekas_deep",
                 "abeta_mim_duara_r", "abeta_mim_duara_l", "abeta_mim_fazekas_peri", "abeta_mim_fazekas_deep",


                 #Lastly, the volumetrics, again as MRI each scan will have its own set; if a additional sweeps are done we'll have three instead of the current 2
                 "abeta_mim_brain_v", "abeta_mim_cerebel_v", "abeta_mim_cerebel_p", "abeta_mim_cortex_v", "abeta_mim_cortex_p", "abeta_mim_caudate_l_v", "abeta_mim_caudate_l_p", "abeta_mim_caudate_r_v", "abeta_mim_caudate_r_p", "abeta_mim_frontal_l_v", "abeta_mim_frontal_l_p", "abeta_mim_frontal_r_v", "abeta_mim_frontal_r_p",
                 "abeta_mim_hippoc_l_v", "abeta_mim_hippoc_l_p", "abeta_mim_hippoc_r_v", "abeta_mim_hippoc_r_p", "abeta_mim_ventric_l_v", "abeta_mim_ventric_l_p", "abeta_mim_ventric_r_v", "abeta_mim_ventric_r_p", "abeta_mim_occip_l_v", "abeta_mim_occip_l_p", "abeta_mim_occip_r_v", "abeta_mim_occip_r_p",
                 "abeta_mim_parietal_l_v", "abeta_mim_parietal_l_p", "abeta_mim_parietal_r_v", "abeta_mim_parietal_r_p", "abeta_mim_putamen_l_v", "abeta_mim_putamen_l_p", "abeta_mim_putamen_r_v", "abeta_mim_putamen_r_p", "abeta_mim_temporal_l_v", "abeta_mim_temporal_l_p", "abeta_mim_temporal_r_v", "abeta_mim_temporal_r_p",

                 #Tau volumetrics
                 "tau_mim_brain_v", "tau_mim_cerebel_v", "tau_mim_cerebel_p", "tau_mim_cortex_v", "tau_mim_cortex_p", "tau_mim_caudate_l_v", "tau_mim_caudate_l_p", "tau_mim_caudate_r_v", "tau_mim_caudate_r_p", "tau_mim_frontal_l_v", "tau_mim_frontal_l_p", "tau_mim_frontal_r_v", "tau_mim_frontal_r_p",
                 "tau_mim_hippoc_l_v", "tau_mim_hippoc_l_p", "tau_mim_hippoc_r_v", "tau_mim_hippoc_r_p", "tau_mim_ventric_l_v", "tau_mim_ventric_l_p", "tau_mim_ventric_r_v", "tau_mim_ventric_r_p", "tau_mim_occip_l_v", "tau_mim_occip_l_p", "tau_mim_occip_r_v", "tau_mim_occip_r_p",
                 "tau_mim_parietal_l_v", "tau_mim_parietal_l_p", "tau_mim_parietal_r_v", "tau_mim_parietal_r_p", "tau_mim_putamen_l_v", "tau_mim_putamen_l_p", "tau_mim_putamen_r_v", "tau_mim_putamen_r_p", "tau_mim_temporal_l_v", "tau_mim_temporal_l_p", "tau_mim_temporal_r_v", "tau_mim_temporal_r_p"
    ),

    temp_drop = c(#The first set is the amyloid specific questions from the PET table (question 6 from NACC) followed by the tau questions
                  "Abnormally elevated amyloid on PET?_PIB_RESULT", "Abnormally elevated amyloid on PET?_PIB_CONFIDENCE", "Hippocampal atrophy (Duara 2-4 on right and/or left)?_PIB_RESULT", "Right hippocampal atrophy (Duara 2-4)?_PIB_RESULT", "Left hippocampal atrophy (Duara 2-4)?_PIB_RESULT", "Structural MRI evidence of FTLD?_PIB_RESULT", "Structural MRI evidence of FTLD?_PIB_CONFIDENCE",
                  "Hippocampal atrophy (Duara 2-4 on right and/or left)?_AV1451_RESULT", "Right hippocampal atrophy (Duara 2-4)?_AV1451_RESULT", "Left hippocampal atrophy (Duara 2-4)?_AV1451_RESULT", "Tau-PET evidence for AD?_AV1451_RESULT", "Tau-PET evidence for AD?_AV1451_CONFIDENCE", "Tau-PET evidence of FTLD?_AV1451_RESULT", "Tau-PET evidence of FTLD?_AV1451_CONFIDENCE", "Structural MRI evidence of FTLD?_AV1451_RESULT", "Structural MRI evidence of FTLD?_AV1451_CONFIDENCE",

                  #As an aside, this would be a good part to interleave any FDG or DAT scan data which is part of question 6
                  #We currently don't have any scans that do this but again, this has been built with scalability in mind

                  #Next is the vascular based table which is basically all MRI so we again have both a amyloid and tau specific set
                  "Large vessel infarct(s)?_PIB_RESULT", "Large vessel infarct(s)?_PIB_LOCATION", "Lacunar infarct(s)?_PIB_RESULT", "Lacunar infarct(s)?_PIB_LOCATION", "Macrohemorrhage(s)?_PIB_RESULT", "Macrohemorrhage(s)?_PIB_LOCATION", "Microhemorrage(s)?_PIB_RESULT", "Microhemorrage(s)?_PIB_LOCATION", "Moderate white matter hyperintensities?(CHS 5-6)_PIB_RESULT", "Extensive white matter hyperintensities?(CHS 7 or 8)_PIB_RESULT", "CHS score (1-8)_PIB_RESULT",
                  "Large vessel infarct(s)?_AV1451_RESULT", "Large vessel infarct(s)?_AV1451_LOCATION", "Lacunar infarct(s)?_AV1451_RESULT", "Lacunar infarct(s)?_AV1451_LOCATION", "Macrohemorrhage(s)?_AV1451_RESULT", "Macrohemorrhage(s)?_AV1451_LOCATION", "Microhemorrage(s)?_AV1451_RESULT", "Microhemorrage(s)?_AV1451_LOCATION", "Moderate white matter hyperintensities?(CHS 5-6)_AV1451_RESULT", "Extensive white matter hyperintensities?(CHS 7 or 8)_AV1451_RESULT", "CHS score (1-8)_AV1451_RESULT"
    ),


    #The remaining entry calls are much smaller, we just fill in blanks for all these as REDCap is solely expecting free text (no radio buttons or drop downs)
    default_response = "",
    uds_recode = c(""),
    uds_null = c(""),
    uds_ver_col = NA,
    reduce_collapse_string = "<br>",
    force_integer = c("tau_mim_duara_r", "tau_mim_duara_l", "tau_mim_fazekas_peri", "tau_mim_fazekas_deep",
                      "abeta_mim_duara_r", "abeta_mim_duara_l", "abeta_mim_fazekas_peri", "abeta_mim_fazekas_deep")

  ))

# Remove *_res_amyl and *_res_tau entries from mim_to_redcap_dict$Imaging
# mim_to_redcap_dict$Imaging <- within(mim_to_redcap_dict$Imaging, {
#   keep_idx <- !grepl("(_res_amyl|_res_tau)$", quest_id)
#   quest_id <- quest_id[keep_idx]
#   quest_num <- quest_num[keep_idx]
# })

