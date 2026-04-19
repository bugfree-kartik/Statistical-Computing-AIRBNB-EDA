# AMS 597 – Inside Airbnb NYC: Data Cleaning (R)
# Mirrors airbnb_data_cleaning.ipynb — requires: tidyverse, lubridate
# Install if needed: install.packages(c("tidyverse", "lubridate", "gridExtra"))

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(gridExtra)
})

options(tibble.width = Inf)

# -----------------------------------------------------------------------------
## 2. Load raw data (change path if your file name differs)
# -----------------------------------------------------------------------------
data_file <- "listings 2 (1).csv"
df_raw <- readr::read_csv(data_file, show_col_types = FALSE)

message("Shape: ", nrow(df_raw), " rows × ", ncol(df_raw), " columns")

# -----------------------------------------------------------------------------
## 3. Initial inspection (uncomment to run)
# -----------------------------------------------------------------------------
# glimpse(df_raw)
# df_raw |> summarise(across(everything(), ~ sum(is.na(.)))) # etc.

# -----------------------------------------------------------------------------
## 4. Data cleaning
# -----------------------------------------------------------------------------

cols_to_drop <- c(
  "listing_url", "scrape_id", "source", "picture_url",
  "host_url", "host_profile_id", "host_profile_url",
  "host_thumbnail_url", "host_picture_url",
  "calendar_last_scraped",
  "estimated_revenue_l365d",
  "calendar_updated",
  "license",
  "neighbourhood",
  "name", "description", "neighborhood_overview",
  "host_about", "host_location",
  "amenities", "host_verifications"
)

df <- df_raw |> dplyr::select(-any_of(cols_to_drop))
message("Columns remaining: ", ncol(df))

# --- 4.2 Percentage columns → proportion in [0, 1]
pct_cols <- c("host_response_rate", "host_acceptance_rate")

parse_pct_string <- function(x) {
  xc <- str_trim(stringr::str_remove(as.character(x), fixed("%")))
  xc[xc %in% c("NA", "nan", "", "NaN")] <- NA_character_
  suppressWarnings(as.numeric(xc) / 100)
}

df <- df |>
  mutate(across(all_of(pct_cols), parse_pct_string))

# --- 4.3 Booleans: t/f → logical
bool_cols <- c(
  "host_is_superhost",
  "host_has_profile_pic",
  "host_identity_verified",
  "has_availability",
  "instant_bookable"
)

map_tf <- function(x) {
  xc <- str_trim(as.character(x))
  case_when(
    xc %in% c("t", "TRUE", "True") ~ TRUE,
    xc %in% c("f", "FALSE", "False") ~ FALSE,
    TRUE ~ NA
  )
}

df <- df |> mutate(across(all_of(bool_cols), map_tf))

# --- 4.4 Dates
date_cols <- c("last_scraped", "host_since", "first_review", "last_review")

safe_parse_datetime <- function(x) {
  xc <- as.character(x)
  # parse_date_time handles common ISO / Airbnb exports
  suppressWarnings(parse_date_time(xc, orders = c("ymd HMS", "ymd HM", "ymd", "mdy HMS", "mdy")))
}

df <- df |>
  mutate(across(all_of(date_cols), safe_parse_datetime))

# --- 4.5 Missing values (non-price)
numeric_fill_median <- c("bathrooms", "bedrooms", "beds")

for (col in numeric_fill_median) {
  m <- median(df[[col]], na.rm = TRUE)
  n_na <- sum(is.na(df[[col]]))
  df[[col]][is.na(df[[col]])] <- m
  message(sprintf("%s: filled %d missing values with median (%s)", col, n_na, format(m)))
}

review_score_cols <- c(
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value"
)

for (col in review_score_cols) {
  m <- median(df[[col]], na.rm = TRUE)
  n_na <- sum(is.na(df[[col]]))
  df[[col]][is.na(df[[col]])] <- m
  message(sprintf("%s: filled %d missing with median (%s)", col, n_na, format(m, digits = 6)))
}

for (col in pct_cols) {
  m <- median(df[[col]], na.rm = TRUE)
  n_na <- sum(is.na(df[[col]]))
  df[[col]][is.na(df[[col]])] <- m
  message(sprintf("%s: filled %d missing with median (%s)", col, n_na, format(m, digits = 6)))
}

for (col in bool_cols) {
  n_na <- sum(is.na(df[[col]]))
  df[[col]][is.na(df[[col]])] <- FALSE
  message(sprintf("%s: filled %d missing with FALSE", col, n_na))
}

df <- df |>
  mutate(
    host_response_time = replace_na(host_response_time, "unknown"),
    reviews_per_month = replace_na(reviews_per_month, 0)
  )

# --- 4.5b Price: parse, invalid → NA, median fill, winsorize 99.5%
price_vec <- df$price
if (is.numeric(price_vec) && !inherits(price_vec, "character")) {
  price_num <- suppressWarnings(as.numeric(price_vec))
} else {
  price_num <- readr::parse_number(as.character(price_vec))
}

missing_before <- sum(is.na(price_num))
nonpositive <- !is.na(price_num) & price_num <= 0
n_bad_sign <- sum(nonpositive)
price_num[nonpositive] <- NA_real_

median_price <- median(price_num, na.rm = TRUE)
price_num[is.na(price_num)] <- median_price

hi_cap <- quantile(price_num, 0.995, na.rm = TRUE)
n_capped <- sum(price_num > hi_cap, na.rm = TRUE)
price_num <- pmin(price_num, hi_cap)

df$price <- price_num

message(sprintf(
  "price: missing before coerce/clean %d; non-positive set to NA: %d",
  missing_before, n_bad_sign
))
message(sprintf(
  "price: median imputation $%.2f; winsor cap at 99.5%% = $%.2f (%d rows capped)",
  median_price, hi_cap, n_capped
))
print(summary(df$price))

# --- 4.6 Duplicates
n_before <- nrow(df)
df <- df |> dplyr::distinct(id, .keep_all = TRUE)
message(sprintf(
  "Duplicate rows removed: %d  |  Remaining: %d",
  n_before - nrow(df), nrow(df)
))

# --- 4.7 Validation
stopifnot(all(df$availability_365 >= 0 & df$availability_365 <= 365, na.rm = TRUE))
stopifnot(all(df$review_scores_rating >= 0 & df$review_scores_rating <= 5, na.rm = TRUE))
stopifnot(all(df$host_response_rate >= 0 & df$host_response_rate <= 1, na.rm = TRUE))
stopifnot(all(df$host_acceptance_rate >= 0 & df$host_acceptance_rate <= 1, na.rm = TRUE))
stopifnot(!any(is.na(df$price)))
stopifnot(all(df$price > 0))
message("All range checks passed.")

# -----------------------------------------------------------------------------
## 5. Summary
# -----------------------------------------------------------------------------
message("\n=== Clean Dataset Overview ===")
message("Rows: ", nrow(df), "   Columns: ", ncol(df))
rem <- df |> summarise(across(everything(), ~ sum(is.na(.)))) |>
  pivot_longer(everything(), names_to = "column", values_to = "n_missing") |>
  filter(n_missing > 0)
if (nrow(rem) == 0) {
  message("No missing values remain (all columns).")
} else {
  message("Remaining missing values:")
  print(rem, n = 50)
}

key_vars <- c(
  "price",
  "room_type", "neighbourhood_cleansed", "neighbourhood_group_cleansed",
  "number_of_reviews", "availability_365", "review_scores_rating",
  "estimated_occupancy_l365d", "reviews_per_month"
)
summary(df |> dplyr::select(any_of(key_vars)))

message("\nRoom type:")
print(table(df$room_type))
message("\nBorough:")
print(table(df$neighbourhood_group_cleansed))

# Histograms (ggplot2)
p1 <- ggplot(df, aes(x = review_scores_rating)) +
  geom_histogram(bins = 40, fill = "steelblue", color = "white") +
  labs(title = "Review Scores Rating", x = "Rating") +
  theme_minimal()

p2 <- ggplot(df, aes(x = availability_365)) +
  geom_histogram(bins = 40, fill = "coral", color = "white") +
  labs(title = "Availability (days/year)", x = "Days") +
  theme_minimal()

p3 <- ggplot(df, aes(x = log1p(number_of_reviews))) +
  geom_histogram(bins = 40, fill = "seagreen", color = "white") +
  labs(title = "Number of Reviews (log1p)", x = "log(1 + reviews)") +
  theme_minimal()

p4 <- ggplot(df, aes(x = price)) +
  geom_histogram(bins = 50, fill = "mediumpurple", color = "white") +
  labs(title = "Nightly price (USD, after cleaning)", x = "Price") +
  theme_minimal()

gridExtra::grid.arrange(p1, p2, p3, p4, ncol = 2)

# -----------------------------------------------------------------------------
## 6. Save
# -----------------------------------------------------------------------------
readr::write_csv(df, "listings_clean.csv")
message("\nClean dataset saved to listings_clean.csv")
message("Final shape: ", nrow(df), " × ", ncol(df))
