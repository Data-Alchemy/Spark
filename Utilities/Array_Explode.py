def flatten_explode_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct' and c[1][:5] != 'array']
    array_columns = [c[0] for c in nested_df.dtypes if c[1][:12] == 'array<struct']
    lvl = 0 
    column_types = nested_df.dtypes
    for c in  column_types:
      column_name = c[0]
      if c[1][:12] == 'array<struct':
        parse_column_name =  column_name+'_Parsed'
        flat_array = nested_df.select(column_name).select("*", explode(column_name).alias(parse_column_name)).drop(column_name).select("*", parse_column_name+".*")
        flat_array_col  = [col(column_name).alias(parse_column_name+'_'+ column_name) for column_name in flat_array.select("*", parse_column_name+".*").drop(parse_column_name).columns]
        flat_array      = flat_array.select(*flat_array_col)
      elif c[1][:6] == 'struct':
        flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
        nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
        flat_array = nested_df.select(flat_cols +
                               [col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_array
