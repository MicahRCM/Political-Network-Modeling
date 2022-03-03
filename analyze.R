library(plyr)
library(tidyverse)
library(rstudioapi)

setwd(dirname(getActiveDocumentContext()$path))

# Parameters
MIN_AUTHOR_COMMENTS <- 10


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

dir <- "./Used Data/"
reddit_csvs <- list.files(dir, pattern="*.csv")

# Loops through all csvs in directory (reddit_csvs)
# Pass in directory address and the data to which everything should be appended
for (i in seq_along(reddit_csvs)) {
  all_data <- count_participation(paste0(dir, reddit_csvs[i]), all_data)
  print(i)
}

# Only keeping authors with a post frequency higher than a threshold
valid_authors <- all_data %>%
  filter(total > MIN_AUTHOR_COMMENTS)

# Input the csv location, the data everything will be appended to, and a list of valid authors
# Outputs data with author, created, votes, subreddit, and flair.
get_valid_author_data <- function(obs_csv_chunk, all_chunks, valid_authors) {
  obs_csv_chunk <- read.csv(obs_csv_chunk, col.names = c("author", "created", "votes", "subreddit", "flair"))
  valid_data <- obs_csv_chunk %>% 
    filter(author %in% valid_authors)
  bind_rows(valid_data, all_chunks)  
}

# Declaring typed dataframe that we will append data to 
valid_author_comments <- data.frame(author = character(),
                                    created = integer(),
                                    votes = integer(),
                                    subreddit = character(),
                                    flair = character())
# Test range # 
for (i in 1:2) {
  valid_author_comments <- get_valid_author_data(paste0(dir, reddit_csvs[i]),
                                                  valid_author_comments,
                                                  valid_authors$author) 
  print(i)
}

test_valid_pcm_authors <- valid_author_comments %>% 
  filter(subreddit == "PoliticalCompassMemes" & flair != "") %>%
  distinct(author)

test_valid_pcm_authors_all_comments <- valid_author_comments %>%
  filter(author %in% test_valid_pcm_authors$author)

test_valid_pcm_authors_all_comments$flair <- str_replace_all(test_valid_pcm_authors_all_comments$flair, "^(.*?)-( )", "")

test_pcm_auth_grouped <- test_valid_pcm_authors_all_comments %>% 
  group_by(author, subreddit, flair) %>%
  summarise(vote_sum = sum(votes),
         count = n())

test_pcm_auth_grouped_2 <- test_pcm_auth_grouped %>% 
  filter(subreddit == "PoliticalCompassMemes") %>%
  distinct() %>%
  ungroup() %>%
  select(author, flair) %>%
  right_join(dplyr::select((test_pcm_auth_grouped %>% 
                              ungroup()), 
                           -c("flair")), by = "author")

test_votes_auths <- test_pcm_auth_grouped_2 %>% 
  select(author, flair, subreddit, vote_sum) %>%
  tidyr::pivot_wider(names_from = subreddit,
                     values_from = vote_sum, 
                     names_prefix = "u_",
                     values_fn = list)

test_comment_count_auths <- test_pcm_auth_grouped_2 %>%
  select(author, flair, subreddit, count) %>%
  pivot_wider(names_from = subreddit, 
              values_from = count, 
              names_prefix = "n_",
              values_fn = list)

test_final_joined_data <- left_join(test_votes_auths, select(test_comment_count_auths, -flair), by = "author")
