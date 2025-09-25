the_plan <-
  drake_plan(

   #Step 1 - name the tables, read in the tables to a list
   table_names = c("./input/tab1.csv", "./input/tab2.csv"),
   input_list = parse_csv(table_names),

   #Step 2 - process header
   mim_header = process_header(input_list[["mim_header"]]),

   #Step 3 - process CSV for UDS on D1
   uds_curr = csv_to_d1_uds(input_list[["all_csvs"]], packet="IVP"),

   #Step 4 - add header to UDS and print to file
   uds_output = uds_to_csv(uds_curr, mim_header, .event="ivp_arm_1")



)
