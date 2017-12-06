
# このモジュール内部では、外部から呼び出される関数とpostgresqlとの中間を担う。
# データは配列または辞書、JSON形式で返却するようにする。
# データベースへのインスタンスはクラスインスタンスで保持する。
# import "./.postgresql.conf"
import psycopg2
import psycopg2.extras
import json
import os

# データベースへの各種インスタンスを保持するクラス。
# 外部へは関与しない。
class PostgresqlHandler():
  def __init__(self):
    self.__config = self.__importConfig()
    connection = self.__setupConnection(self.__config)
    # 暗黙的なCOMMITを行う
    connection.autocommit = True
    # DictCursorを使用するための拡張機能
    self.__cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    self.__setupTable(self.__cursor, self.__config)

  # 非公開メソッド
  def __importConfig(self):
    path = os.path.dirname(os.path.abspath(__file__))
    with open('{}/.postgresql.conf'.format(path), 'r') as f:
      config = json.load(f)
      print("debug:: config: {}".format(config))
      return config

  def __setupConnection(self, c):
    connection = psycopg2.connect(
      "host={host} port={port} dbname={dbname} user={user} password={password}"
      .format(host=c['host'], port=c['port'], dbname=c['dbname'], user=c['user'], password=c['password'])
    )
    print("debug:: connection: {}".format(connection))
    return connection

  def __setupTable(self, cursor, config):
    cursor.execute("select version()")
    print(cursor.fetchone())
    # パラメーター形成　start
    columns = self.__config['columns']
    params = ''
    for param in columns.keys():
      params += "{colomn} {type},".format(colomn=param, type=columns[param])
    params = params[:-1]
    # パラメーター形成　end
    try:
      cursor.execute("create table {tname}(id int,{columns})".format(tname=config['tname'], columns=params))
    except psycopg2.ProgrammingError as e:
      print("pass setupTable")
      pass

  # 公開メソッド

  # ユーザーデータを格納する
  # rule: 格納すべきカラムスをそのまま文字列で渡す。
  def setUserData(self, stringUserData):
    try:
      # idカウンタ
      idCount = 0
      self.__cursor.execute("select * from {}".format(self.__config['tname']))
      for i in self.__cursor:
        idCount += 1
      
      self.__cursor.execute(
        "insert into {tname} values({id},{columns})"
        .format(tname=self.__config['tname'], id=idCount, columns=stringUserData)
      )
      print('success insert')
    except:
      print("rule: 格納すべきカラムスをカンマで区切った文字列で渡す")
      print("for example: 'line_id','user_name','user_mail'")

  # ユーザーデータを取得する
  # rule: カラムを配列に格納して返却する, 各カラムは辞書として格納する
  def getUserData(self):
    try:
      userDataArray = []
      columns = self.__config['columns']
      self.__cursor.execute("select * from {}".format(self.__config['tname']))
      for row in self.__cursor:
        userDict = {}
        for column in columns.keys():
          userDict[column] = row[column]
        userDataArray.append(userDict)
      print("debug:: userDataArray: {}".format(userDataArray))
      return userDataArray
    except:
      print("error getUserData")
      raise


psqlHandler = PostgresqlHandler()
