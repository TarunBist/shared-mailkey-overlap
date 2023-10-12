## Description: for any specified shared campaign, get a breakdown of mailkey 
## overlaps between the campaign of interest + other shared campaigns

library(DBI)
library(RPostgres)
library(tidyverse)
library(readxl)
library(lubridate)
library(data.table)
library(glue)



# Redshift connect --------------------------------------------------------

# launch cloudflare daemon in separate terminal:
# cloudflared access tcp --hostname "bastion.sharelocalmedia.com" --url "localhost:5439" --destination "production.cx0kgmj3w6wh.us-east-1.redshift.amazonaws.com:5439"

con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = 'dev',
                      host = 'localhost',
                      port = 5439,
                      user = Sys.getenv("redshift_user"),
                      password = Sys.getenv("redshift_pwd"),
                      sslmode = 'require')

# res <- dbSendQuery(con, "select * from pg_tables where schemaname = 'mail_planner'")
# dbFetch(res)
# dbClearResult(res)

# establish connection to tables
# organizations <- tbl(con, dbplyr::in_schema("mail_planner", "organizations"))
# shared_envelopes <- tbl(con, dbplyr::in_schema("mail_planner", "shared_envelopes"))
# client_shared_envelopes <- tbl(con, dbplyr::in_schema("mail_planner", "client_shared_envelopes"))
# clients <- tbl(con, dbplyr::in_schema("mail_planner", "clients"))

# # mailFiles <- tbl(con, dbplyr::in_schema("production", "mailings"))
# campaigns <- tbl(con, dbplyr::in_schema("mail_planner", "campaigns"))

mailFiles <- tbl(con, dbplyr::in_schema("production", "raw_mailings"))
campaigns <- tbl(con, dbplyr::in_schema("dbt_prod", "shared_campaigns_by_clients"))

# get shared campaigns
campaigns.shared <- campaigns %>%
  filter(campaign_type == "shared") %>%
  select(campaign_id,
         slm_id,
         campaign_name,
         print_job_number = print_job_numbers,
         expected_in_homes) %>%
  # head(20) %>%
  collect()

# fix print job number column
campaigns.shared <- campaigns.shared %>%
  mutate(print_job_number = gsub("[^0-9]", "", print_job_number),
         expected_in_homes = gsub("[^0-9-]", "", expected_in_homes)) %>%
  filter(print_job_number != "" & !is.na(print_job_number)) %>%
  distinct(slm_id, .keep_all = TRUE)



# Set parameters that will determine what overlap we want -----------------
# campaign ID of the campaign of interest; reference this
recentCamp <- "SLMS158"

natlOnly <- FALSE
citiesOnly <- FALSE
recentMarket <- TRUE
default <- FALSE

# safety check
if(natlOnly & citiesOnly) {
  stop("Both natl and cities are set to TRUE. Consider switching over to
       the default option")
}



# Select the campaign you want overlaps for --------------------------------
mailing_recent <- campaigns.shared %>%
  filter(grepl(recentCamp, slm_id))

# safety check
if(nrow(mailing_recent) == 0) {
  stop("This campaign ID doesn't exist in the shared campaigns table. Double check
       the campaign ID provided")
}



# Select other campaigns that overlaps will be calculated against ----------

mailingsForComparions <- campaigns.shared %>%
  filter(date(expected_in_homes) < unique(date(mailing_recent$expected_in_homes))
         & date(expected_in_homes) >= unique((date(mailing_recent$expected_in_homes) - 89)))

if (natlOnly) {
  mailingsForComparions <- mailingsForComparions %>%
    filter(grepl("Nat", campaign_name))
}

if (citiesOnly) {
  mailingsForComparions <- mailingsForComparions %>%
    filter(grepl("Cit", campaign_name))
}

if (recentMarket) {
  # if you want to retain the most recent corresponding market, run this chunk ---
  
  sharedType_recent <- gsub("[^A-Za-z]", "", unique(mailing_recent$campaign_name))
  
  mailingsForComparions <- mailingsForComparions %>%
    mutate(sharedType = gsub("[^A-Za-z]","", campaign_name)) %>%
    filter(grepl(sharedType_recent, sharedType))
  
  recent_mailkeys <- mailing_recent %>%
    distinct(slm_id, .keep_all = TRUE) %>%
    mutate(tmp.id = gsub("(SLMS.*-)+?", "", slm_id)
           ,
           camp_id = gsub("\\-.*","", slm_id)
    ) %>%
    filter(!tmp.id == "SEED") %>%
    left_join(select(mailing_recent, slm_id,
                     # print_job_number,
                     inHomeDate= expected_in_homes, opportunity_name = campaign_name))
  
  old_mailkeys <- mailingsForComparions %>%
    distinct(slm_id, .keep_all = TRUE) %>%
    mutate(tmp.id = gsub("(SLMS.*-)+?", "", slm_id),
           camp_id = gsub("\\-.*","", slm_id)) %>%
    filter(!tmp.id == "SEED") %>%
    left_join(select(mailingsForComparions, slm_id,
                     # print_job_number,
                     inHomeDate= expected_in_homes, opportunity_name = campaign_name))
  
  mailingsForComparions <- old_mailkeys %>%
    filter(tmp.id %in% recent_mailkeys$tmp.id) %>%
    group_by(tmp.id) %>%
    filter(inHomeDate == max(inHomeDate))
  
}

if (default) {
  mailingsForComparions <- mailingsForComparions
}



# Pull mailing data -------------------------------------------------------
# campaign of interest
dat_recent <- mailFiles %>%
  filter(print_job_number == !!unique(mailing_recent$print_job_number)) %>%
  select(misc, print_job_number,
         address_hash_v2) %>%
  # head(10) %>%
  collect()

# can't use print_job_number to join, use the slm_id instead
# also, get reed of seeds
dat_recent <- dat_recent %>%
  # mutate(slm_id = gsub("\\-.*","", misc)) %>%
  filter(!grepl("SEED", misc))

# safety check
if(length(setdiff(unique(mailing_recent$slm_id), unique(dat_recent$misc))) > 0) {
  stop(glue("The data for the following campaign doesn't exist in the mailing table yet:
            {setdiff(unique(mailing_recent$slm_id), unique(dat_recent$slm_id))}"))
}

# append campaign data to the addresses
dat_recent <- dat_recent %>%
  left_join(select(mailing_recent, slm_id,
                   # print_job_number,
                   inHomeDate= expected_in_homes,opportunity_name = campaign_name),
            by = c("misc" = "slm_id"))

mailingCirc_recent <- dat_recent %>% count(misc, opportunity_name)

# older campaigns
if (recentMarket) {
  dat_old <- mailFiles %>%
    filter(print_job_number %in% !!mailingsForComparions$print_job_number &
             misc %in% !!mailingsForComparions$slm_id) %>%
    select(misc, print_job_number,
           address_hash_v2) %>%
    collect()
} else {
  dat_old <- mailFiles %>%
    filter(print_job_number %in% !!mailingsForComparions$print_job_number) %>%
    select(misc, print_job_number,
           address_hash_v2) %>%
    collect()
}


# repeat for older campaigns
dat_old <- dat_old %>%
  # mutate(slm_id = gsub("\\-.*","", misc)) %>%
  filter(!grepl("SEED", misc)) %>%
  filter(!grepl("-P\\d{1,5}", misc)) # get rid of PCP data

# safety check
if (recentMarket) {
  if(length(setdiff(unique(mailingsForComparions$slm_id), unique(dat_old$misc))) > 0) {
    stop(glue("The data for the following campaign doesn't exist in the mailing table yet:
            {setdiff(unique(mailing_old$slm_id), unique(dat_old$slm_id))}"))
  }
} else {
  if(length(setdiff(unique(mailingsForComparions$slm_id), unique(dat_old$misc))) > 0) {
    stop(glue("The data for the following campaign doesn't exist in the mailing table yet:
            {setdiff(unique(mailing_old$slm_id), unique(dat_old$slm_id))}"))
  }
}


dat_old <- dat_old %>%
  left_join(select(mailingsForComparions, slm_id,
                   # print_job_number,
                   inHomeDate= expected_in_homes,opportunity_name = campaign_name),
            by =c("misc" = "slm_id"))

mailingCirc_old <- dat_old %>% count(misc, opportunity_name)



# Create Overlaps ---------------------------------------------------------

# pre-processing; get rid of dupes within a mailing 
dat_recent <- dat_recent %>%
  distinct(address_hash_v2, .keep_all = TRUE)

dat_old <- dat_old %>%
  distinct(opportunity_name, address_hash_v2, .keep_all = TRUE)

dat_old_split <- dat_old %>%
  as.data.table() %>%
  split(by = "opportunity_name")


overlap <- lapply(dat_old_split, function(x, y = dat_recent){
  x %>%
    select(c(address_hash_v2, misc_old = misc, opportunity_name)) %>%
    inner_join(select(y,c(address_hash_v2, misc_new = misc, opportunity_name)),
               by = "address_hash_v2")
})


# overlap_automate <- function(x, y) {
#   x %>%
#     select(c(addresshash, misc_old = misc, opportunity_name)) %>%
#     inner_join(select(y,c(addresshash, misc_new = misc, opportunity_name)), by = "addresshash")
# }
# 
# overlap <- lapply(dat_old_split, overlap_automate, y = dat_recent)

overlap_summ <- overlap %>%
  rbindlist() %>%
  group_by(misc_new, misc_old) %>%
  count(misc_new, misc_old) %>%
  ungroup() %>%
  left_join(select(mailingCirc_recent, misc_new = misc, opportunity_name_new = opportunity_name, 
                   mailingCirc = n), by = "misc_new") %>%
  left_join(select(mailingCirc_old, misc_old = misc, opportunity_name_old = opportunity_name),
            by = "misc_old") %>%
  mutate(overlap_pct = n/mailingCirc * 100) %>%
  mutate(slm_id_new = gsub("(-)+?.*", "", misc_new),
         slm_id_old = gsub("(-)+?.*", "", misc_old))

overlap_summ_split <- overlap_summ %>%
  as.data.table() %>%
  split(by = "slm_id_old")

overlap_summ_wide <- lapply(overlap_summ_split, function(x){
  x %>%
    select(misc_new, misc_old, overlap_pct) %>%
    pivot_wider(names_from = misc_old, values_from = overlap_pct, values_fill = 0)
})



# Create heatmap visualization --------------------------------------------

# if we're comparing our most recent campaign in question against multiple campaigns
# then squishing it all in one graph will be overkill
# subdivide the heatmap into multiple heatmaps where they're split up based on campaign

heatmapPlot <- function(a) {
  
  print(ggplot(a, aes(x = misc_new, y = misc_old, fill = overlap_pct)) +
          geom_tile(color = "white",
                    lwd = 0.5,
                    linetype = 1) +
          # geom_text(aes(label = overlap_pct), color = "white", size = 4) +
          scale_fill_gradientn(colors = c("darkred", "white")
                               # ,
                               # limits = c(floor(min(a$overlap_pct)), ceiling(max(a$overlap_pct))),
                               # breaks = seq(floor(min(a$overlap_pct)), ceiling(max(a$overlap_pct)), length.out = 5)
          ) +
          labs(x = unique(a$opportunity_name_new),
               y = unique(a$opportunity_name_old),
               title = "Mailkey overlap between shared campaigns") +
          theme(plot.title = element_text(hjust = 0.5),
                axis.text.x = element_text(angle = 90))
  )
  
  # ggsave(paste0(unique(a$slm_id_new), "_", unique(a$slm_id_old),"_mailkey_overlap.png"),
  #        width = 20,
  #        height = 10)
  
}

plotOutputs <- lapply(overlap_summ_split, heatmapPlot)


# Write outputs -----------------------------------------------------------

# check if directory exists; if not, create it here
outputPath <- c("grid_output", "ggplots")

lapply(outputPath, function(x) {
  if (!exists(x)) {
    dir.create(x)
  }
})


lapply(names(overlap_summ_split), function(x) {
  fwrite(overlap_summ_split[[x]], glue("{outputPath[1]}/{recentCamp}_{x}_mailkey_overlap.csv"))
})

lapply(names(plotOutputs), function(x) {
  ggsave(glue("{outputPath[2]}/{recentCamp}_{x}_mailkey_overlap.png"), width = 20, height = 10)
})

