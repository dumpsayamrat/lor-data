from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from dagster import op
import pyspark
from delta import *
import dagster_pyspark
from delta.tables import *
from pyspark.sql.functions import *
import os
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, ArrayType

def find_champion_names(selector, parent):
  champ_names = parent.find_elements(By.XPATH, ".//div[@class='{}']".format(selector))
  return list(map(lambda n: n.text, champ_names));

def gen_deck_name(champion_names):
  return ",".join(champion_names)

def gen_deck_champions(champion_names):
  champions = []
  for name in champion_names:
    champions.append({
      "champ_name": name,
      "region": "n/a"
    })
  return champions

def find_deck_againts(selector, stat_element):
  best_deck_container = stat_element.find_element(By.CLASS_NAME, selector)
  best_deck_elements = stat_element.find_element(By.CLASS_NAME, "gridtwostat") \
    .find_elements(By.XPATH, ".//div[@class='bestcolorstats metacontent metacontentbest ng-star-inserted']")
  rows = []
  for deck in best_deck_elements:
    champion_names = find_champion_names("champname best", deck)
    deck_name = gen_deck_name(champion_names)
    champions = gen_deck_champions(champion_names)
    play_info = deck.find_elements(By.CLASS_NAME, "playrate")
    info_text_list = list(map(lambda n: n.text, play_info))
    matches = info_text_list[0].split("\n")[1]
    win_rate = info_text_list[1].split("\n")[1]
    rows.append(Row(deck_name=deck_name, champions=champions, win_rate=win_rate, matches=matches))
  return rows

@op(
  description="""This will scrape lor win rate data""",
  required_resource_keys={"pyspark", "pyspark_step_launcher"}
)
def get_lor_win_rate(context) -> DataFrame:

  # creates master dataframe
  # df = pd.DataFrame(columns=["Player", "Salary", "Year"])
  champion_schema = ArrayType(StructType([StructField("champ_name", StringType()), StructField("region", StringType())]))
  matches_meta_schema = ArrayType(StructType([
    StructField("deck_name", StringType()),
    StructField("champions", champion_schema),
    StructField("win_rate", StringType()),
    StructField("matches", StringType()),
  ]))
  schema = StructType([
    StructField("deck_name", StringType()),
    StructField("champions", champion_schema),
    StructField("matches", StringType()),
    StructField("win_rate", StringType()),
    StructField("play_rate", StringType()),
    StructField("matches_best", matches_meta_schema, True),
  ])
  rows = []

  driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

  url = "https://runeterra.ar/stats"
  driver.get(url)
  stats = WebDriverWait(driver, timeout=10).until(lambda d: d.find_elements(By.XPATH, "//div[starts-with(@id,'stat')]"))
  # stats = driver.find_elements(By.XPATH, "//div[starts-with(@id,"stat")]")
  context.log.info("stats: {}".format(len(stats)))
  for stat in stats:
    meta = stat.find_element(By.XPATH, ".//div[starts-with(@id,'meta')]")
    champion_names = find_champion_names("champname", meta)
    deck_name = gen_deck_name(champion_names)
    champions = gen_deck_champions(champion_names)

    play_info = meta.find_elements(By.CLASS_NAME, "playrate")
    info_text_list = list(map(lambda n: n.text, play_info))
    matches = info_text_list[1].split("\n")[1]
    win_rate = info_text_list[2].split("\n")[1]
    play_rate = info_text_list[3].split("\n")[1]

    matches_best = find_deck_againts("metastatdetailsbest", stat)

    rows.append(Row(deck_name=deck_name, champions=champions, win_rate=win_rate, matches=matches, play_rate=play_rate, matches_best=matches_best))

  driver.close()
  return context.resources.pyspark.spark_session.createDataFrame(rows, schema)

@op(required_resource_keys={"pyspark", "pyspark_step_launcher"})
def write_to_file(players: DataFrame) -> DataFrame:
  # players.write.save("./temp/test-par")
  players.toPandas().to_csv("./temp/lor-winrate.csv")
  return players
