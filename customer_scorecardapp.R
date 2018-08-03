library(shiny)
library(DBI)
library(RPostgreSQL)
library(dotenv)
library(ggplot2)
library(MASS)

source("data.R") 



# User interface object
# Define UI for app that draws a histogram ----
ui <- fluidPage(
  
  titlePanel("Customer Scorecard"),
  
  sidebarLayout(position = "left",
                sidebarPanel(
                      
                      selectInput("scorecard.email", "Customer Type", choices = c("Business", "Personal", "All")),
                  
                      tableOutput("scorecard.key"),
                      
                      actionButton("scorecard.run", label = "Run"),
                      
                      # Button
                      downloadButton("scorecard.download", label = "Download")
                      
                      , width = 4
                ),
                
                
                mainPanel( 
                  
                  fluidRow(

                    DT::dataTableOutput("customer_scorecard.table")
                    
                  ), width = 7
                  
                )
)
  )

  


# Server function
# Define server logic required to draw a histogram ----
server <- function(input, output, session) {
  
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
                                           Maximum_Score = c(20, 12, 8, 4, 2, 1, 1, 1, 1, 50)))
    
     
    # Define dataset 
      scorecard <- switch(input$scorecard.email,
                          "Business" = fetch.business(),
                          "Personal" = fetch.personal(),
                          "All" = fetch.all())
      
    output$customer_scorecard.table <- DT::renderDataTable(scorecard)


    # Downloadable csv of selected dataset ----
    
    output$scorecard.download <- downloadHandler(
      
      filename = function() {
        paste("Customer_Scorecard", "_", input$scorecard.email, "_", Sys.Date(), ".csv", sep = "")
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

