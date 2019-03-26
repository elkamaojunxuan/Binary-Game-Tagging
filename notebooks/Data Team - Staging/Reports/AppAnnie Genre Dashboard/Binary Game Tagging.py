# Databricks notebook source
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, LongType, DoubleType

import numpy as np
import re

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Game_Tagging-e8cc2.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table_name = "game_tagging_more"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# DBTITLE 1,Raw data curation
pdf = spark.table('game_tagging_more').toPandas()
new_cols = [column.replace(' ','_').replace('Which_game_is_it?','game') for column in df.columns]
pdf.columns=new_cols

tag_pd = pd.Series(new_cols)[pd.Series(new_cols).str.contains('do_you_see',case=False)]
tag_cat = [re.search('What_(.*)\(s\)',cat).group(1).replace('-','_') for cat in list(tag_pd)]
tag_idx = list(tag_pd.index)

rank_pd = pd.Series(new_cols)[pd.Series(new_cols).str.contains('rank',case=False)]
rank_cat = [re.search('each_(.*)_in',cat).group(1)+'_RANK' for cat in list(rank_pd)]
rank_idx = list(rank_pd.index)
end_idx = pdf.columns.get_loc('What_is_the_role_of_CHARACTER(s)_in_{{Q1}}?')

all_cat = tag_cat + rank_cat
all_idx = tag_idx + rank_idx
all_idx.append(end_idx)

for i in range(len(all_idx)-1):
  new_cols[all_idx[i]]=all_cat[i]
  
to_modify = new_cols.copy()
replacements = list(pdf.iloc[0])

for indexes in zip(all_idx, all_idx[1:]):
  for idx in range(*indexes):
    to_modify[idx] = replacements[idx]
    to_modify[idx] = str(new_cols[range(*indexes)[0]]) + '_' + str(replacements[idx])
    
new_cols = to_modify.copy()    
new_columns = [s.replace(' _ ','_').replace(' / ','/').replace(' ','-').replace('(','').replace(')','').replace('{','').replace('}','')
               for s in new_cols]
pdf.columns=new_columns
pdf = pdf[pdf['game']!='Response']

df = df.toDF(*new_columns)
schema_to_pass = df.schema

# shcema_to_pass = df.toDF(*new_columns).schema

game_idx = pdf.columns.get_loc("game")
df_tag = pdf.iloc[:,[game_idx,*range(all_idx[0],rank_idx[0])]]
df_rank = pdf.iloc[:,[game_idx,*range(rank_idx[0],all_idx[-1])]]

## ART Style has no ranking
art_start = pdf.columns.get_loc('THEME_Flag/To-Be-Defined-Please-Specify')
art_end =  pdf.columns.get_loc('ART_STYLE_Flag/To-Be-Defined-Please-Specify')
df_art = pdf.iloc[:,[game_idx,*range(art_start+1,art_end+1)]]

# COMMAND ----------

# DBTITLE 1,Concatenate columns by category and save into wide table
wide = pdf.copy()
for i in range(len(tag_cat)):
  wide[tag_cat[i]] = wide.iloc[:,[*range(all_idx[i],all_idx[i+1])]].apply(lambda x: ' | '.join(x.dropna().values.tolist()), axis=1)
  
col_wide = ['game',*tag_cat]
df_wide = wide[col_wide]

wide_schema = StructType([StructField("game", StringType(), True), 
                         StructField("GENRE", StringType(), True),
                         StructField("SUB_GENRE", StringType(), True),
                         StructField("CORE_ACTION_PHASE", StringType(), True),
                         StructField("META", StringType(), True),
                         StructField("THEME", StringType(), True),
                         StructField("ART_STYLE", StringType(), True),
                         ])

spark.createDataFrame(df_wide, wide_schema).write.mode('overwrite').format("delta").option("overwriteSchema", "true").saveAsTable('game_tagging_wide')

# COMMAND ----------

df_stack = pd.DataFrame(df_tag.stack()).reset_index().set_index('level_0')
game_list = pd.DataFrame(df_stack[df_stack['level_1']=='game'][0])

df_stack_prep = pd.merge(df_stack, game_list, left_index=True, right_index=True)
df_stack_game = df_stack_prep[df_stack_prep['level_1']!='game']
df_stack_game['category'] = df_stack_game['level_1'].apply(lambda x: x.rsplit('_',1)[0])
col_long = ['0_y','category','0_x']
df_long = df_stack_game[col_long]
df_long.columns =  ['game','category','tag']

# COMMAND ----------

# df_stack = pd.DataFrame(df_rank.stack()).reset_index().set_index('level_0')
# game_list = df_stack[df_stack['level_1']=='game'][0].reset_index(drop=True)

# df_stack_prep = df_stack.join(game_list, lsuffix='_rank', rsuffix='_game')
# df_stack_game = df_stack_prep[df_stack_prep['level_1']!='game']
# df_stack_game.columns=['category_tag', 'rank','game']

# df_stack_game['category'], df_stack_game['tag'] = df_stack_game['category_tag'].str.split('_RANK_',1).str
# df_stack_game = df_stack_game.drop(columns=['category_tag'])

# ## Adding Art Style separatly since it is not ranked
# art_melt = pd.melt(df_art, 
#         id_vars='game',
#         value_vars=df_art.columns[1:],
#         var_name='art_style',  
#         value_name='temp').dropna().drop(columns='temp')
# art_melt['tag'] = art_melt['art_style'].str.replace('ART_STYLE_','')
# art_melt['category']='ART_STYLE'
# art_melt['rank']='1'
# art_stack= art_melt.drop(columns='art_style')

# col = ['game', 'category', 'tag', 'rank']
# df_stack_game = df_stack_game[col]
# art_stack = art_stack[col]
# df_full = pd.concat([df_stack_game, art_stack])
# # df_full = df_full[df_full['tag']!='[Insert-text-from-Other]']

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, LongType, DoubleType

long_schema = StructType([StructField("game", StringType(), True), 
                          StructField("category", StringType(), True),
                          StructField("tag", StringType(), True),
#                           StructField("rank", StringType(), True),
                         ])

spark.createDataFrame(df_long, long_schema).write.mode('overwrite').format("delta").option("overwriteSchema", "true").saveAsTable('game_tagging_long')

# COMMAND ----------

# spark.sql('''
# select * from game_tagging_long
# full outer join game_tagging_wide using (game)
# order by 1,2,3,4,5,6,7,8,9
# ''').write.mode("overwrite").format('delta').option("overwriteSchema","true").saveAsTable("game_tagging_combined")

# COMMAND ----------

# genre = pdf.iloc[:,[game_idx,*range(tag_idx[0],tag_idx[1])]].copy()                    
# sub_genre = pdf.iloc[:,[game_idx,*range(tag_idx[1],tag_idx[2])]].copy()
# cpa = pdf.iloc[:,[game_idx,*range(tag_idx[2],tag_idx[3])]].copy()
# meta = pdf.iloc[:,[game_idx,*range(tag_idx[3],tag_idx[4])]].copy()
# theme = pdf.iloc[:,[game_idx,*range(tag_idx[4],tag_idx[5])]].copy()
# art = pdf.iloc[:,[game_idx,*range(tag_idx[5],tag_idx[6])]].copy()
# genre_rank = pdf.iloc[:,[game_idx,*range(tag_idx[6],tag_idx[7])]].copy()
# sub_genre_rank = pdf.iloc[:,[game_idx,*range(tag_idx[7],tag_idx[8])]].copy()
# cpa_rank = pdf.iloc[:,[game_idx,*range(tag_idx[8],tag_idx[9])]].copy()
# meta_rank = pdf.iloc[:,[game_idx,*range(tag_idx[9],tag_idx[10])]].copy()
# theme_rank = pdf.iloc[:,[game_idx,*range(tag_idx[10],tag_idx[11])]].copy()


# g_melt = pd.melt(genre, 
#         id_vars='game',
#         value_vars=genre.columns[1:],
#         var_name='genre',  
#         value_name='temp').dropna().drop(columns='temp')

# s_melt = pd.melt(sub_genre, 
#         id_vars='game',
#         value_vars=sub_genre.columns[1:],
#         var_name='sub_genre',  
#         value_name='temp').dropna().drop(columns='temp')

# c_melt = pd.melt(cpa, 
#         id_vars='game',
#         value_vars=cpa.columns[1:],
#         var_name='core_action_phase',  
#         value_name='temp').dropna().drop(columns='temp')

# m_melt = pd.melt(meta, 
#         id_vars='game',
#         value_vars=meta.columns[1:],
#         var_name='meta',  
#         value_name='temp').dropna().drop(columns='temp')

# t_melt = pd.melt(theme, 
#         id_vars='game',
#         value_vars=theme.columns[1:],
#         var_name='theme',  
#         value_name='temp').dropna().drop(columns='temp')

# a_melt = pd.melt(art, 
#         id_vars='game',
#         value_vars=art.columns[1:],
#         var_name='art_style',  
#         value_name='temp').dropna().drop(columns='temp')

# g_melt['category']=g_melt.columns[1]
# s_melt['category']=s_melt.columns[1]
# c_melt['category']=c_melt.columns[1]
# m_melt['category']=m_melt.columns[1]
# t_melt['category']=t_melt.columns[1]
# a_melt['category']=a_melt.columns[1]

# col = ['game','tag','category']

# g_melt.columns=col
# s_melt.columns=col
# c_melt.columns=col
# m_melt.columns=col
# t_melt.columns=col
# a_melt.columns=col

# all_game_tag = pd.concat([g_melt,s_melt,c_melt,m_melt,t_melt,a_melt],ignore_index=True)

# gr_melt = pd.melt(genre_rank, 
#         id_vars='game',
#         value_vars=genre_rank.columns[1:],
#         var_name='genre',  
#         value_name='temp').dropna().drop(columns='temp')

# sr_melt = pd.melt(sub_genre_rank, 
#         id_vars='game',
#         value_vars=sub_genre_rank.columns[1:],
#         var_name='sub_genre',  
#         value_name='temp').dropna().drop(columns='temp')

# cr_melt = pd.melt(cpa_rank, 
#         id_vars='game',
#         value_vars=cpa_rank.columns[1:],
#         var_name='core_action_phase',  
#         value_name='temp').dropna().drop(columns='temp')

# mr_melt = pd.melt(meta_rank, 
#         id_vars='game',
#         value_vars=meta_rank.columns[1:],
#         var_name='meta',  
#         value_name='temp').dropna().drop(columns='temp')

# tr_melt = pd.melt(theme_rank, 
#         id_vars='game',
#         value_vars=theme_rank.columns[1:],
#         var_name='theme',  
#         value_name='temp').dropna().drop(columns='temp')

# gr_melt['category']=gr_melt.columns[1]
# sr_melt['category']=sr_melt.columns[1]
# cr_melt['category']=cr_melt.columns[1]
# mr_melt['category']=mr_melt.columns[1]
# tr_melt['category']=tr_melt.columns[1]

# col_rank = ['game','rank','category']

# gr_melt.columns=col_rank
# sr_melt.columns=col_rank
# cr_melt.columns=col_rank
# mr_melt.columns=col_rank
# tr_melt.columns=col_rank

# all_game_rank = pd.concat([gr_melt,sr_melt,cr_melt,mr_melt,tr_melt],ignore_index=True)