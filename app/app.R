library(shiny)
library(DBI)
library(RPostgreSQL)
library(dotenv)
library(ggplot2)
library(MASS)
library(dplyr)
library(stringr)
library(stringi)
library(memoise)
library(rsconnect)

source("data.R") 



# User interface object
# Define UI for app that draws a histogram ----
ui <- fluidPage(
  
  titlePanel("Customer Scorecard"),
  
  sidebarLayout(position = "left",
                sidebarPanel(
                      
                      selectInput("scorecard.email.type", "Customer Type", choices = c("Business", "Personal", "All")),
                      
                      #textInput("scorecard.email.address", "Enter Email Address"),
                  
                      tableOutput("scorecard.key"),
                      
                      actionButton("scorecard.run", label = "Run"),
                      
                      # Button
                      downloadButton("scorecard.download", label = "Download")
                      
                      , width = 4
                ),
                
                
                mainPanel( 
                  
                  fluidRow(

                    DT::dataTableOutput("customer_scorecard.table"),
                    
                    #DT::dataTableOutput("customer_scorecard.drilldown")
                    
                    plotOutput("customer_scorecard.drilldown")
                    
                  ), width = 7
                  
                )
)
  )

  


# Server function
# Define server logic required to draw a histogram ----
server <- function(input, output, session) {
  
  # Progress bar
  progress <- Progress$new(session, min=1, max=15)
  on.exit(progress$close())
  
  progress$set(message = 'Loading data',
               detail = 'This may take a while...')
  
  for (i in 1:15) {
    progress$set(value = i)
    Sys.sleep(0.5)
  }
  
  hist.scores <- fetch.hist()
  
  
  observeEvent(input$scorecard.run, {  
  
    # Progress bar
    progress <- Progress$new(session, min=1, max=15)
    on.exit(progress$close())
    
    progress$set(message = 'Calculation in progress',
                 detail = 'This may take a while...')
    
    for (i in 1:15) {
      progress$set(value = i)
      Sys.sleep(0.5)
    }
    
     # Build score key table 
  
     output$scorecard.key <- renderTable(cbind.data.frame(Event_Name = c('dashboard_daily_pageviews', 'engagement_frequency', 'number_of_named_apps', 'tech_info', 'active_app', 'multiple_env_flag', 'collaborators', 'tut_doc_pv', 'hit_soft_limit', 'total'), 
                                           Maximum_Score = c(40, 24, 16, 8, 4, 2, 2, 2, 2, 100)))
    
     
    # Define dataset 
      scorecard <- switch(input$scorecard.email.type,
                          "Business" = fetch.business(),
                          "Personal" = fetch.personal(),
                          "All" = fetch.all())
      
      #hist.scores <- fetch.hist()
    
    # display the data that is available to be drilled down
    output$customer_scorecard.table <- DT::renderDataTable(scorecard, selection = 'single')

    # subset the records to the row that was clicked
    drilldata <- reactive({
      shiny::validate(
        need(length(input$customer_scorecard.table_rows_selected) == 1, "Select row to drill down!")
      )    
    
      # subset the summary table and extract the column to subset on
      # if you have more than one column, consider a merge instead
      # NOTE: the selected row indices will be character type so they
      #   must be converted to numeric or integer before subsetting
      selected_email <- scorecard[as.integer(input$customer_scorecard.table_rows_selected), ]$email
      
      hist.scores %>% 
      filter(str_detect(email, selected_email)) %>% 
      dplyr::select(email, end_date, score) %>% 
        dplyr::rename(date = end_date)
    })
    
    # display the subsetted data
    #output$customer_scorecard.drilldown <- DT::renderDataTable(drilldata())
    
    output$customer_scorecard.drilldown <- renderPlot({
      
      ggplot(data = drilldata(), aes(x = date, y = score)) + geom_line(color = "red") +
        coord_cartesian(ylim = c(0, 100)) + labs(title = "Daily Score", subtitle = paste(scorecard[as.integer(input$customer_scorecard.table_rows_selected), ]$email) , x ="Date", y = "Score") +
        theme(plot.title = element_text(hjust = 0.5))
    })
    
      

    # Downloadable csv of selected dataset ----
    
    output$scorecard.download <- downloadHandler(
      
      filename = function() {
        paste("Customer_Scorecard", "_", input$scorecard.email.type, "_", Sys.Date(), ".csv", sep = "")
      },
      
      content = function(file) {
        write.csv(scorecard, file, row.names = FALSE)
      }
      
    )
  
    })
}


# Call to the shinyApp function
# Create Shiny app ----
shinyApp(ui = ui, server = server)

