library(plyr)
library(tidyverse)
library(rstudioapi)

setwd(dirname(getActiveDocumentContext()$path))

# Parameters
MIN_AUTHOR_COMMENTS <- 5


# Assign initial empty df to store author and total numbers of posts/comments
all_data <- data.frame(author = character(), total = integer())

# Combines grouped author data and adds post counts
count_participation <- function(obs_csv_chunk, all_chunks) {
  obs_csv_chunk <- read.csv(obs_csv_chunk, col.names = c("author", "created", "votes", "subreddit", "flair"))
  author_counts <- obs_csv_chunk %>% 
    group_by(author) %>%
    dplyr::summarise(count = n())
  
  comments_2 <- full_join(author_counts, all_chunks, by = "author")
  comments_2$total <- rowSums(comments_2[,c("total", "count")], na.rm = TRUE)
  comments_2 <- subset(comments_2, select = c(author, total))

}

dir <- "./"
reddit_csvs <- list.files(dir, pattern="*.csv")

# Loops through all csvs in directory (reddit_csvs)
for (i in seq_along(reddit_csvs)) {
  all_data <- count_participation(paste0(dir, reddit_csvs[i]), all_data)
  print(i)
}

# Only keeping authors with a post frequency higher than a threshold
valid_authors <- all_data %>%
  filter(total > MIN_AUTHOR_COMMENTS)

reddit_csvs <- 

