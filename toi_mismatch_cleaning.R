all_games <-
  c(
    20092010,
    20102011,
    20112012,
    20122013,
    20132014,
    20142015,
    20152016,
    20162017,
    20172018,
    20182019,
    20192020,
    20202021,
    20212022,
    20222023,
    20232024,
    20242025
  ) |>
  purrr::map(nhlScrapeR::get_full_season_schedule_api) |>
  dplyr::bind_rows() |>
  dplyr::filter(
    session %in% c(2, 3),
    # game_id > 2020030116,
    game_date < lubridate::today()
  ) #, game_id >= 2019030100) |>


all_games |>
  dplyr::full_join(
    tibble::tibble(
      game_id =
        list.files("raw_files/") |>
        stringr::str_extract("\\d{10}") |>
        as.integer()
      ) |>
      dplyr::group_by(game_id) |>
      dplyr::tally()
  ) |>
  dplyr::select(game_id, game_date, n) |>
  dplyr::filter(is.na(n) | n != 16 |is.na(game_date))
  View()



all_games |>
  dplyr::filter(
    season %in% c(
      20092010
      # 20102011
      # 20112012,
      # 20122013,
      # 20132014,
      # 20142015,
      # 20152016,
      # 20162017,
      # 20172018,
      # 20182019,
      # 20192020,
      # 20202021,
      # 20212022,
      # 20222023,
      # 20232024,
      # 20242025
    ),
    game_id < 2010021060,
    session == 2
  ) |>
  # dplyr::filter(game_id < 2015030135) |>
  dplyr::pull(game_id) |>
  sort() |>
  rev() |>
  purrr::walk(
    function(g_id) {
      start_time <- Sys.time()

      dplyr::bind_rows(
        nhlScrapeR::get_game_shifts_raw_html(g_id, "home", F) |>
          nhlScrapeR::extract_shifts_from_raw_shifts_html(g_id, "home", F),
        nhlScrapeR::get_game_shifts_raw_html(g_id, "away", F) |>
          nhlScrapeR::extract_shifts_from_raw_shifts_html(g_id, "away", F)
      ) |>
        readr::write_csv(
          "raw_files/html_results_shifts_{g_id}.csv" |> glue::glue()
        )

      print("{g_id} Time Elapsed: {Sys.time() - start_time}" |> glue::glue())
    }
  )

all_games |>
  dplyr::filter(
    season %in% c(
      20092010,
      20102011,
      20112012,
      20122013,
      20132014,
      20142015,
      20152016,
      20162017,
      20172018,
      20182019,
      20192020,
      20202021,
      20212022,
      20222023,
      20232024,
      20242025
    ),
    # game_id < 2021030113,
    session == 2
  ) |>
  # dplyr::filter(game_id < 2015030135) |>
  dplyr::pull(game_id) |>
  sort() |>
  rev() |>
  # head() |>
# 2019030016 |>

# c(2016030185, 2015030143, 2014030135, 2013030236, 2012030162, 2012030153, 2010030181, 2009030165) |>
  purrr::walk(
    function(g_id) {
      start_time <- Sys.time()

      res <-
        list(
          api_results =
            list(
              meta =
                readr::read_csv(
                  "raw_files/api_results_meta_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_date = readr::col_date(),
                      start_time_utc = readr::col_datetime(),
                      venue_name = readr::col_character(),
                      venue_place_name = readr::col_character(),
                      home_team = readr::col_character(),
                      away_team = readr::col_character(),
                      home_team_place_name = readr::col_character(),
                      .default = readr::col_integer()
                    )
                ),
              rosters =
                readr::read_csv(
                  "raw_files/api_results_rosters_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      api_id = readr::col_integer(),
                      sweater_number = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              scratches =
                readr::read_csv(
                  "raw_files/api_results_scratches_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      api_id = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              coaches =
                readr::read_csv(
                  "raw_files/api_results_coaches_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              referees =
                readr::read_csv(
                  "raw_files/api_results_referees_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              linesmen =
                readr::read_csv(
                  "raw_files/api_results_linesmen_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              shfits =
                readr::read_csv(
                  "raw_files/api_results_shifts_{g_id}.csv" |> glue::glue(),
                  col_types = readr::cols(.default = readr::col_integer())
                ),
              pbp =
                readr::read_csv(
                  "raw_files/api_results_pbp_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_strength_state = readr::col_character(),
                      home_team_def_zone = readr::col_character(),
                      event_type = readr::col_character(),
                      event_type_detail = readr::col_character(),
                      penalty_class = readr::col_character(),
                      event_reason_1 = readr::col_character(),
                      event_reason_2 = readr::col_character(),
                      zone_code = readr::col_character(),
                      .default = readr::col_integer()
                    )
                )
            ),
          html_results =
            list(
              meta =
                readr::read_csv(
                  "raw_files/html_results_meta_{g_id}.csv" |> glue::glue(),
                  col_types = readr::cols(.default = readr::col_integer())
                ),
              rosters =
                readr::read_csv(
                  "raw_files/html_results_rosters_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      sweater_number = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              scratches =
                readr::read_csv(
                  "raw_files/html_results_scratches_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      sweater_number = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              coaches =
                readr::read_csv(
                  "raw_files/html_results_coaches_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      .default = readr::col_character()
                    )
                ),
              referees =
                readr::read_csv(
                  "raw_files/html_results_referees_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      name = readr::col_character()
                    )
                ),
              linesmen =
                readr::read_csv(
                  "raw_files/html_results_linesmen_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      game_id = readr::col_integer(),
                      name = readr::col_character()
                    )
                ),
              shifts =
                readr::read_csv(
                  "raw_files/html_results_shifts_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      venue = readr::col_character(),
                      .default = readr::col_integer()
                    )
                ),
              pbp =
                readr::read_csv(
                  "raw_files/html_results_pbp_{g_id}.csv" |> glue::glue(),
                  col_types =
                    readr::cols(
                      event_strength = readr::col_character(),
                      event_type = readr::col_character(),
                      event_description = readr::col_character(),
                      .default = readr::col_integer()
                    )
                ) |>
                dplyr::mutate(
                  event_description = event_description |> tidyr::replace_na("")
                )
            )
        )

      print(res)

      clean_res <- nhlScrapeR::clean_game_details_all_sources(res, verbose = F)

      readr::write_csv(
        clean_res$game_metadata_clean,
        "clean_files/metadata_{g_id}.csv" |> glue::glue()
      )
      readr::write_csv(
        clean_res$game_rosters_clean,
        "clean_files/rosters_{g_id}.csv" |> glue::glue()
      )
      readr::write_csv(
        clean_res$scratches_clean,
        "clean_files/scratches_{g_id}.csv" |> glue::glue()
      )
      readr::write_csv(
        clean_res$coaches_clean,
        "clean_files/coaches_{g_id}.csv" |> glue::glue()
      )
      readr::write_csv(
        clean_res$officials_clean,
        "clean_files/officials_{g_id}.csv" |> glue::glue()
      )
      readr::write_csv(
        clean_res$shifts_clean,
        "clean_files/shifts_{g_id}.csv" |> glue::glue()
      )
      readr::write_csv(
        clean_res$pbp_clean,
        "clean_files/pbp_{g_id}.csv" |> glue::glue()
      )

      print("{g_id} Time Elapsed: {Sys.time() - start_time}" |> glue::glue())
    }
  )


  # dplyr::pull(game_id) |>
toi_mismatches <-
  all_playoff_games |>
  dplyr::filter(
    season %in% c(
      20092010,
      20102011,
      20112012,
      20122013,
      20132014,
      20142015,
      20152016,
      20162017,
      20172018
      # 20182019
      # 20192020
      # 20202021,
      # 20212022,
      # 20222023,
      # 20232024,
      # 20242025
    ),
    game_id %in% c(
      2017030242, 2016030185, 2015030143, 2014030135, 2013030236, 2012030162,
      2012030153, 2010030181, 2009030165
    )
    # game_id == 2017030242
  ) |>
  # head() |>
  dplyr::arrange(season, game_id) |>
  dplyr::mutate(
    shifts =
      purrr::map(
        game_id,
        function(g_id) {
          readr::read_csv(
            "clean_files/shifts_{g_id}.csv" |> glue::glue(),
            col_types = c("i", "i", "c", "i", "i", "c", "i", "c")
          ) |>
            dplyr::mutate(duration = shift_end - shift_start) |>
            dplyr::group_by(api_id) |>
            dplyr::summarise(toi_scrape = sum(duration))
        }
      )
  ) |>
  tidyr::unnest(shifts) |>
# 2024030111 |>
  # purrr::map(
  #   function(g_id) {
  #     readr::read_csv(
  #       "clean_files/shifts_{g_id}.csv" |> glue::glue(),
  #       col_types = c("i", "i", "c", "i", "i", "c", "i", "c")
  #     ) |>
  #       dplyr::mutate(duration = shift_end - shift_start) |>
  #       dplyr::group_by(game_id, api_id) |>
  #       dplyr::summarise(toi_scrape = sum(duration))
  #   }
  # ) |>
  # dplyr::bind_rows() |>
  dplyr::group_by(season, api_id) |>
  tidyr::nest() |>
  dplyr::mutate(
    data =
      purrr::pmap(
        list(
          s = season,
          api = api_id,
          df = data
        ),
        # api_id,
        # data,
        function(s, api, df) {
          df |>
            dplyr::full_join(
              "https://api-web.nhle.com/v1/player/{api}/game-log/{s}/3" |>
                glue::glue() |>
                httr::GET() |>
                httr::content(type = "text", encoding = "UTF-8") |>
                jsonlite::fromJSON() |>
                purrr::pluck("gameLog") |>
                tibble::as_tibble() |>
                dplyr::group_by(game_id = gameId) |>
                dplyr::transmute(
                  game_id = gameId,
                  toi =
                    toi |>
                    stringr::str_split(":") |>
                    purrr::flatten_chr() |>
                    as.integer() |>
                    magrittr::multiply_by(c(60, 1)) |>
                    sum()
                  # .groups = "drop"
                ) |>
                dplyr::ungroup(),
              by = dplyr::join_by(game_id)
            )
        }
      )
  ) |>
  tidyr::unnest(data) |>
  dplyr::filter(
    toi_scrape != toi
  )
  # View("spot test")


View(toi_mismatches)





toi_mismatches_reg <-
  all_games |>
  dplyr::filter(
    season %in% c(
      # 20092010,
      # 20102011,
      # 20112012,
      # 20122013,
      # 20132014,
      # 20142015,
      # 20152016,
      # 20162017,
      # 20172018,
      # 20182019,
      # 20192020,
      # 20202021,
      # 20212022,
      # 20222023,
      # 20232024,
      20242025
    ),
    session == 2
    # game_id == 2017030242
  ) |>
  # head() |>
  dplyr::arrange(season, game_id) |>
  dplyr::mutate(
    shifts =
      purrr::map(
        game_id,
        function(g_id) {
          readr::read_csv(
            "clean_files/shifts_{g_id}.csv" |> glue::glue(),
            col_types = c("i", "i", "c", "i", "i", "c", "i", "c")
          ) |>
            dplyr::mutate(duration = shift_end - shift_start) |>
            dplyr::group_by(api_id) |>
            dplyr::summarise(toi_scrape = sum(duration))
        }
      )
  ) |>
  tidyr::unnest(shifts) |>
  dplyr::group_by(season, api_id) |>
  tidyr::nest() |>
  dplyr::mutate(
    data =
      purrr::pmap(
        list(
          s = season,
          api = api_id,
          df = data
        ),
        function(s, api, df) {
          game_log <-
            "https://api-web.nhle.com/v1/player/{api}/game-log/{s}/2" |>
            glue::glue() |>
            httr::GET() |>
            httr::content(type = "text", encoding = "UTF-8") |>
            jsonlite::fromJSON() |>
            purrr::pluck("gameLog") |>
            tibble::as_tibble()

          if (nrow(game_log) > 0) {
            df |>
              dplyr::full_join(
                game_log |>
                  dplyr::group_by(game_id = gameId) |>
                  dplyr::transmute(
                    game_id = gameId,
                    toi =
                      toi |>
                      stringr::str_split(":") |>
                      purrr::flatten_chr() |>
                      as.integer() |>
                      magrittr::multiply_by(c(60, 1)) |>
                      sum()
                  ) |>
                  dplyr::ungroup(),
                by = dplyr::join_by(game_id)
              )
          } else {
            df
          }
        }
      )
  ) |>
  tidyr::unnest(data) |>
  dplyr::filter(
    toi_scrape != toi
  )




for (seas in rev(c(
  20092010,
  20102011,
  20112012,
  20122013,
  20132014,
  20142015,
  20152016,
  20162017
  # 20172018,
  # 20182019,
  # 20192020,
  # 20202021,
  # 20212022,
  # 20222023,
  # 20232024
))) {
  message(seas)

  toi_mismatches_reg <-
    toi_mismatches_reg |>
    dplyr::bind_rows(
      all_games |>
        dplyr::filter(
          season == seas,
          session == 2,
          !game_id %in% c(2009020081, 2009020658, 2009020714, 2009020885)
        ) |>
        # head() |>
        dplyr::arrange(season, game_id) |>
        dplyr::mutate(
          shifts =
            purrr::map(
              game_id,
              function(g_id) {
                readr::read_csv(
                  "clean_files/shifts_{g_id}.csv" |> glue::glue(),
                  col_types = c("i", "i", "c", "i", "i", "c", "i", "c")
                ) |>
                  dplyr::mutate(duration = shift_end - shift_start) |>
                  dplyr::group_by(api_id) |>
                  dplyr::summarise(toi_scrape = sum(duration))
              }
            )
        ) |>
        tidyr::unnest(shifts) |>
        dplyr::group_by(season, api_id) |>
        tidyr::nest() |>
        dplyr::mutate(
          data =
            purrr::pmap(
              list(
                s = season,
                api = api_id,
                df = data
              ),
              function(s, api, df) {
                game_log <-
                  "https://api-web.nhle.com/v1/player/{api}/game-log/{s}/2" |>
                  glue::glue() |>
                  httr::GET() |>
                  httr::content(type = "text", encoding = "UTF-8") |>
                  jsonlite::fromJSON() |>
                  purrr::pluck("gameLog") |>
                  tibble::as_tibble()

                if (nrow(game_log) > 0) {
                  df |>
                    dplyr::full_join(
                      game_log |>
                      dplyr::group_by(game_id = gameId) |>
                      dplyr::transmute(
                        game_id = gameId,
                        toi =
                        toi |>
                        stringr::str_split(":") |>
                        purrr::flatten_chr() |>
                        as.integer() |>
                        magrittr::multiply_by(c(60, 1)) |>
                        sum()
                      ) |>
                      dplyr::ungroup(),
                      by = dplyr::join_by(game_id)
                    )
                } else {
                  df
                }
              }
            )
        ) |>
        tidyr::unnest(data) |>
        dplyr::filter(
          toi_scrape != toi
        )
    )
}





tibble::tibble(
  game_id =
  rosters =
)





toi_mismatches_reg <-
  toi_mismatches_reg |>
  dplyr::ungroup() |>
  dplyr::select(season, game_date, game_id, api_id, toi, toi_scrape) |>
  dplyr::mutate(api_id = as.integer(api_id)) |>
  dplyr::inner_join(
    toi_mismatches_reg$game_id |>
      unique() |>
      # head() |>
      purrr::map(
        function(g_id) {
          readr::read_csv(
            "clean_files/rosters_{g_id}.csv" |> glue::glue(),
            col_types =
              readr::cols(
                game_id = readr::col_integer(),
                api_id = readr::col_integer(),
                sweater_number = readr::col_integer(),
                .default = readr::col_character()
              )
          )
        }
      ) |>
      dplyr::bind_rows() |>
      dplyr::select(
        game_id, api_id, name, venue, sweater_number
      )
  ) |>
  dplyr::mutate(diff = toi - toi_scrape)


toi_mismatches_reg |>
  dplyr::filter(
    !game_id %in% c(
      ## fixes done
      2021021189, 2021020452, 2021020427, 2021020416, 2021020326, 2020020865,
      2020020860, 2020020858, 2020020857, 2020020810, 2020020762, 2020020748,
      2020020526, 2020020367, 2020020252, 2020020124, 2019021076, 2019021053,
      2019021047, 2019021043, 2019021019, 2019020963, 2019020852, 2019020842,
      2019020808, 2019020775, 2019020772, 2019020726, 2019020722, 2019020710,
      2019020708, 2019020674, 2019020665, 2019020628, 2019020591, 2019020580,
      2019020549, 2019020535, 2019020479, 2019020477, 2019020475, 2019020457,
      2019020447, 2019020418, 2019020410, 2019020365, 2019020331, 2019020316,
      2019020259, 2019020234, 2019020221, 2019020201, 2019020178, 2019020169,
      2019020129, 2019020072, 2019020030, 2019020021, 2019020019, 2019020014,
      2019020011, 2018021052, 2018020963, 2018020890, 2018020732, 2018020681,
      2018020592, 2018020555, 2018020397, 2018020164, 2018020144, 2018020086,
      2018020081, 2018020072, 2017021267, 2017021083, 2017020820, 2017020666,
      2017020434, 2016021194, 2016021163, 2016020936, 2016020915, 2016020856,
      2016020511, 2016020421, 2016020419, 2016020163, 2016020139, 2016020099,
      2015021224, 2015021049, 2015021003, 2015020969, 2015020918, 2015020900,
      2015020866, 2015020849,
      ## ignore for now/revisit

      ## leave the way they are
      2022021067, 2019021034, 2019021000, 2019020941, 2019020937, 2019020898,
      2019020815, 2019020754, 2019020743, 2019020700, 2019020696, 2019020688,
      2019020666, 2019020639, 2019020626, 2019020623, 2019020559, 2019020509,
      2019020497, 2019020351, 2019020347, 2019020284, 2019020191, 2019020188,
      2019020181, 2019020170, 2019020160, 2019020156, 2019020149, 2019020050,
      2019020040, 2019020023, 2019020010, 2018021212, 2018020786, 2018020764,
      2018020351, 2016020510
    )
  ) |>
  # dplyr::select(season, game_id) |>
  # dplyr::distinct() |>
  # dplyr::group_by(season) |>
  # dplyr::tally()
  dplyr::filter(game_id == max(game_id)) |>
  dplyr::select(-season, game_date) |>
  View()

g <- 2015020825

## clean shifts not in api
readr::read_csv(
  "clean_files/shifts_{g}.csv" |> glue::glue(),
  col_types = readr::cols(
    team = readr::col_character(),
    shift_start_zone = readr::col_character(),
    shift_end_zone = readr::col_character(),
    .default = readr::col_integer()
  )
) |>
  dplyr::select(-c(game_id, shift_start_zone, shift_end_zone)) |>
  dplyr::anti_join(
    readr::read_csv(
      "raw_files/api_results_shifts_{g}.csv" |> glue::glue(),
      col_types = readr::cols(.default = readr::col_integer())
    ),
    by = c("api_id" = "event_player_1", "shift_start", "shift_end")
  ) |>
  dplyr::left_join(
    readr::read_csv("clean_files/rosters_{g}.csv" |> glue::glue()) |>
      dplyr::select(name, api_id),
    by = c("api_id")
  ) |>
  dplyr::arrange(api_id, shift_start)

## api shifts not in clean shifts
readr::read_csv(
  "raw_files/api_results_shifts_{g}.csv" |> glue::glue(),
  col_types = readr::cols(.default = readr::col_integer())
) |>
  dplyr::anti_join(
    readr::read_csv(
      "clean_files/shifts_{g}.csv" |> glue::glue(),
      col_types = readr::cols(
        team = readr::col_character(),
        shift_start_zone = readr::col_character(),
        shift_end_zone = readr::col_character(),
        .default = readr::col_integer()
      )
    ),
    by = c("event_player_1" = "api_id", "shift_start", "shift_end")
  ) |>
  dplyr::left_join(
    readr::read_csv("clean_files/rosters_{g}.csv" |> glue::glue()) |>
      dplyr::select(name, api_id),
    by = c("event_player_1" = "api_id")
  ) |>
  dplyr::arrange(event_player_1, shift_start)

readr::read_csv("clean_files/pbp_{g}.csv" |> glue::glue()) |> View()
readr::read_csv("clean_files/shifts_{g}.csv" |> glue::glue()) |> View()


readr::read_csv("raw_files/api_results_shifts_2018020786.csv") |> View()

