# Databricks notebook source
# MAGIC %sh 
# MAGIC apt-get clean
# MAGIC rm -rf /var/lib/apt/lists/*
# MAGIC apt-get clean
# MAGIC apt-get update
# MAGIC apt-get install -y gdal-bin libgdal-dev

# COMMAND ----------

install.packages("leaflet")

# COMMAND ----------

library(shiny)
library(leaflet)

r_colors <- rgb(t(col2rgb(colors()) / 255))
names(r_colors) <- colors()

ui <- fluidPage(
  leafletOutput("mymap"),
  p(),
  actionButton("recalc", "New points")
)

server <- function(input, output, session) {

  points <- eventReactive(input$recalc, {
    cbind(rnorm(40) * 2 + 13, rnorm(40) + 48)
  }, ignoreNULL = FALSE)

  output$mymap <- renderLeaflet({
    leaflet() %>%
      addProviderTiles(providers$Stamen.TonerLite,
        options = providerTileOptions(noWrap = TRUE)
      ) %>%
      addMarkers(data = points())
  })
}

shinyApp(ui, server)
