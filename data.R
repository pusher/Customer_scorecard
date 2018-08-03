library(DBI)
library(RPostgreSQL)
library(dotenv)
library(dplyr)

load_dot_env(file = ".env")
user <- Sys.getenv("LUSERNAME")
password <- Sys.getenv("LPASSWORD")
host <- Sys.getenv("HOST")
port <- Sys.getenv("PORT")
db.name <- Sys.getenv("DB_NAME")

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = db.name,
                 host = host, port = port,
                 user = user, password = password)

# Obtain SQL query function

getSQL <- function(filepath, channels.table, first.touch.table, splash.page.views.table, web.sessions.table) {
  channels.table <- dbGetQuery(con, "select table_name FROM information_schema.tables where table_schema = 'looker_scratch' and table_name like '%channels' limit 1")
  first.touch.table <- dbGetQuery(con,"select table_name FROM information_schema.tables where table_schema = 'looker_scratch' and table_name like '%first_touch' and table_name not like '%elements_first_touch' limit 1")
  splash.page.views.table <- dbGetQuery(con, "select table_name FROM information_schema.tables where table_schema = 'looker_scratch' and table_name like '%splash_page_views' limit 1")
  web.sessions.table <- dbGetQuery(con, "select table_name FROM information_schema.tables where table_schema = 'looker_scratch' and table_name like '%web_sessions' and table_name not like '%elements_web_sessions' limit 1")
  
  f <- read.delim(filepath, sep='\n', header = F)
  sql = ''
  # loop through outside vector
  for (line in f) {
    # loop through inside vectors that contain the text of the line
    for (line.text in line) {
      if(grepl("--",line.text) == TRUE) {
        line.text <- paste(sub("--","/*",line.text),"*/")
      }
      sql <- paste(sql, line.text)  
    }   
  }
  sql <- gsub("LOOKER_SCRATCH_CHANNELS_TABLE", paste("looker_scratch.", channels.table, sep=""), sql)
  sql <- gsub("LOOKER_SCRATCH_FIRST_TOUCH_TABLE", paste("looker_scratch.", first.touch.table, sep=""), sql)
  sql <- gsub("LOOKER_SCRATCH_SPLASH_PAGE_VIEWS_TABLE", paste("looker_scratch.", splash.page.views.table, sep=""), sql)
  sql <- gsub("LOOKER_SCRATCH_WEB_SESSIONS_TABLE", paste("looker_scratch.", web.sessions.table, sep=""), sql)
  
  
  return(sql)
}

fetch.business <- function() {
# Use SQL query to form the data for scorecard
scorecard <- dbGetQuery(con, getSQL("./customer_scorecard.sql"))

scorecard.filt <-  scorecard %>% 
  filter(bus_email_flag == 1) %>% 
  select(account_id, email, dashboard_daily_pageviews, engagement_frequency, number_of_named_apps, tech_info, active_app, multiple_env_flag,
         collaborators, tut_doc_pv, hit_soft_limit, total)

scorecart.sorted <- scorecard.filt %>% 
  arrange(-total)

return(scorecart.sorted)
}

fetch.personal <- function() {
  # Use SQL query to form the data for scorecard
  scorecard <- dbGetQuery(con, getSQL("./customer_scorecard.sql"))
  
  scorecard.filt <-  scorecard %>% 
    filter(bus_email_flag == 0) %>% 
    select(account_id, email, dashboard_daily_pageviews, engagement_frequency, number_of_named_apps, tech_info, active_app, multiple_env_flag,
           collaborators, tut_doc_pv, hit_soft_limit, total)
  
  scorecart.sorted <- scorecard.filt %>% 
    arrange(-total)
  
  return(scorecart.sorted)
}

fetch.all <- function() {
  # Use SQL query to form the data for scorecard
  scorecard <- dbGetQuery(con, getSQL("./customer_scorecard.sql"))
  
  scorecard.filt <-  scorecard %>% 
    select(account_id, email, dashboard_daily_pageviews, engagement_frequency, number_of_named_apps, tech_info, active_app, multiple_env_flag,
           collaborators, tut_doc_pv, hit_soft_limit, total)
  
  scorecart.sorted <- scorecard.filt %>% 
    arrange(-total)
  
  return(scorecart.sorted)
}


