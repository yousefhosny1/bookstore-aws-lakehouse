class Loader:
    def load(self, data, mode, path, table_name):
        data.write.mode(mode).option('header', True).csv(path + f'/{table_name}')

