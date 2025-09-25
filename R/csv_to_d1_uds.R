#Step 3
#Process the list of CSVs for the D1 specific uploads
#Make the uds_questions object is appropriately referencing the MIM_uds_name field (e.g. 6a, 6b)


csv_to_d1_uds <-
  function(csv_list, uds_version, MIM_uds_name="NACC ITEM ID", MIM_uds_entry="RESULT"){

    #Start by extracting all the valid D1 fields from the set of CSVs
    temp_input <-
      lapply(csv_list, function(.df){

        #Pull rows that correspond to UDS questions using the uds_questions
        .df <- .df[which(.df[[MIM_uds_name]] %in% uds_questions[[uds_version]][["quest_num"]]),]

        #Return NULL if there aren't any valid UDS rows
        if(nrow(.df)==0) return(NULL)


        #Recode the results using dplyr::recode (note, this requires the triple-bang on the uds_key dictionary for replacement to handle unquote splicing)
        uds_input <- dplyr::recode(.df[[MIM_uds_entry]], !!!uds_key)


        #Take the results and turn it into a new dataframe
        uds_input <- data.frame(t(uds_input))

        #Pass the appropriate names
        names(uds_input) <- uds_questions[[uds_version]][["quest_id"]][which(.df[[MIM_uds_name]] %in% uds_questions[[uds_version]][["quest_num"]])]

        return(uds_input)
      })

    #Take the list in temp_input, drop the nulls, cbind the rest
    temp_input <- purrr::compact(temp_input)
    do.call(cbind, temp_input)

  }






