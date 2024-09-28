import pandas as pd

def clean_advance_stats_teams(url):

  # Extraer las tablas de la url
  tables = pd.read_html(url, header=1)

  # Seleccionar la tabla advanced stats
  df = tables[10].copy()

  # Quitarnos las filas y columnas que no nos interesan 
  df = df.iloc[1:-1, 1:-3]

  # Obtener las columnas a eliminar
  cols_drop = [col for col in df.columns if 'Unnamed:' in col or 'Awards' in col]
  df.drop(cols_drop, axis=1, inplace=True)

  # Renombrar las columnas del dataframe
  df.columns = [col.replace('.1', '_vs') if col.endswith('.1') else col  for col in df.columns]

  # Quitar el asterisco de los nombres de los equipos
  df['Team'] = df.Team.apply(lambda x: x.replace('*', ''))

  return df





def clean_per_game_teams(url, playoffs=False):

  # Obtenemos las tablas de la url
  tables = pd.read_html(url)

  # Seleccionamos la tabla Per Game o playoffs 
  if playoffs == True:
    df = tables[2].copy()
  else:
    df = tables[1].copy()

  # Eliminamos la columna Rk
  df.drop('Rk', axis=1, inplace=True)

  # Cambiamos el nombre de la columna de los jugadores
  df.rename(columns={'Unnamed: 1':'player'}, inplace=True)

  # Convertimos  el nombre de las columnas a minúscula
  df.columns = [col.lower()  for col in df.columns]

  return df





def select_year_players_bf(url, year1, year2):

  # Añadimos los años a la url 
  url_years = [url[:49] +str(age) + url[53:] for age in list(reversed(range(year1, year2)))]

  # Obtenemos los datos de cada url 
  dfs = []
  for url in url_years:
    df = transform_data_all_players_bf(url)
    df['year'] = url[49:53]
    df['year'] = pd.to_numeric(df['year'])
    dfs.append(df)

  # Hacemos la concatenación de todos los dataframes
  df = pd.concat([dfs[0], dfs[1]])
    
  for i in range(2, len(dfs)):
      df = pd.concat([df, dfs[i]])

  return df





def select_year_team(url, year1, year2):

  # Añadimos los años a la url 
  url_years = [url[:-9] +str(age) + '.html' for age in list(reversed(range(year1, year2)))]

  # Obtenemos los datos de cada url 
  dfs = []
  for url in url_years:
    df = clean_per_game_teams(url)
    df['year'] = url[47:-5]
    dfs.append(df)

  # Hacemos la concatenación de todos los dataframes
  df = pd.concat([dfs[0], dfs[1]])
    
  for i in range(2, len(dfs)):
      df = pd.concat([df, dfs[i]])

  return df





def drop_duplicate_player(df):
    
    # Nombres de los jugadores duplicados - En algunas tablas la columna 'Tm' se llama 'Team' asi que vamos a poner un condicional para quitarnos ese problema
    if df.columns[2] == 'Team':
      # Hay varios jugadores que tienen las columnas 3TM o  2TM en lugar de TOT
      names = df[(df.Team =='TOT') | (df.Team == '2TM') | (df.Team == '3TM')].Player.values
      idx_good = df[(df.Team =='TOT') | (df.Team == '2TM') | (df.Team == '3TM')].index.values
    else:
      names = df[(df.Tm =='TOT') | (df.Tm == '2TM') | (df.Tm == '3TM')].Player.values
      # Los índices con los que nos queremos quedar
      idx_good = df[(df.Tm =='TOT') | (df.Tm == '2TM') | (df.Tm == '3TM')].index.values
    
    # Creamos lista vacía donde añadiremos los índices
    idx=[]

    # Iteramos sobre los nombre de la lista names
    for name in names:
        # Añadimos a la lista vacía los índices de las filas igual a ese nombre
        idx.append(df[df.Player == name].index.values)
        
    # Creamos la lista vacía donde irán los índices que queremos borrar
    drop_idx = []
    
    # El primer bucle itera sobre la lista de los índices duplicados 
    for i in idx:
        # El segundo bucle itera sobre los valores de cada lista del primer bucle
        for j in i:
            if j not in idx_good:
                drop_idx.append(j)
    
                
    df.drop(drop_idx, axis=0, inplace=True)
    #Para resetaer los indices
    df.reset_index(inplace=True, drop=True)
                
    return df





def transform_data_all_players_bf(url):

  # Obtener los datos de la url 
  tables = pd.read_html(url)
  df = tables[0].copy()

  # Obtener los índices donde aparecen las columnas
  idx_drop = df[df.Rk == 'Rk'].index
  df.drop(idx_drop, axis=0, inplace=True)

  # Eliminar las columnas innecesarias
  cols_drop = [col for col in df.columns if (col.__contains__('Unnamed:')) | (col=='Rk') | (col == 'Awards')]
  df.drop(cols_drop, axis=1, inplace=True)

  # Convertir a variables numéricas. En algunas tablas la columna 'Tm' se llama 'Team' asi que vamos a añadir 'Team' a nuestra lista de columnas categoricas
  cols_cat = ['Player', 'Pos', 'Tm', 'Team']
  for col in df.columns:
    if col not in cols_cat:
      df[col] = pd.to_numeric(df[col])

  # Solucionar el problema de las posiciones -> Este problema no existe actualmente en basketball reference
  #### df['Pos'] = df.Pos.apply(lambda x: x.split('-')[0])

  # Eliminar los jugadore duplicados
  df  = drop_duplicate_player(df)

  return df




