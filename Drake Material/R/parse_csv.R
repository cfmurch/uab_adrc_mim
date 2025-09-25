#Step 1 - read in the csv sets to a list, return the MIM header and the list



parse_csv <- function(input_files){

  csv_list <-
    lapply(input_files, function(.csv){

      #Pull first line as header
      #Drop any extra NA's since CSV files are messy, easiest with a colSums check
      header_curr <- read.csv(.csv, header = FALSE, nrow = 1, check.names = FALSE)
      header_curr <- header_curr[, colSums(is.na(header_curr)) != nrow(header_curr)]

      #Pull in the rest as a csv
      csv_curr <- read.csv(.csv, skip = 1, header = TRUE, check.names = FALSE)
      csv_curr <- csv_curr[, colSums(is.na(csv_curr)) != nrow(csv_curr)]

      return(list(header = header_curr, csv = csv_curr))
    })

  #Make sure headers equal - print error return NULL if not
  all_headers <- lapply(csv_list, function(.entry) .entry[["header"]])

  if(length(unique(all_headers))==1){ mim_header <- all_headers[[1]]
  } else {
    print("MIM headers not equal for all tables")
    return(NULL)
  }

  #Assuming single header, compress the csv's into a single list, return that along with a single header
  all_csvs <- lapply(csv_list, function(.entry) .entry[["csv"]])


  return(list(mim_header=mim_header, all_csvs=all_csvs))
}




#
#
# #The prior function used by the drake design
#
#
# parse_csv <- function(input_files){
#
#   csv_list <-
#     lapply(input_files, function(.csv){
#
#       #Pull first line as header
#       #Drop any extra NA's since CSV files are messy, easiest with a colSums check
#       header_curr <- read.csv(.csv, header = FALSE, nrow = 1, check.names = FALSE)
#       header_curr <- header_curr[, colSums(is.na(header_curr)) != nrow(header_curr)]
#
#       #Pull in the rest as a csv
#       csv_curr <- read.csv(.csv, skip = 1, header = TRUE, check.names = FALSE)
#       csv_curr <- csv_curr[, colSums(is.na(csv_curr)) != nrow(csv_curr)]
#
#       return(list(header = header_curr, csv = csv_curr))
#     })
#
#   #Make sure headers equal - print error return NULL if not
#   all_headers <- lapply(csv_list, function(.entry) .entry[["header"]])
#
#   if(length(unique(all_headers))==1){ mim_header <- all_headers[[1]]
#   } else {
#     print("MIM headers not equal for all tables")
#     return(NULL)
#   }
#
#   #Assuming single header, compress the csv's into a single list, return that along with a single header
#   all_csvs <- lapply(csv_list, function(.entry) .entry[["csv"]])
#
#
#   return(list(mim_header=mim_header, all_csvs=all_csvs))
# }


