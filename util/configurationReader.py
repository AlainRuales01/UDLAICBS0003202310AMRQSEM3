from jproperties import Properties
import traceback


def readDataBaseConfigurations():
    try:
        configurations = Properties()
        with open('configurations/DBConfiguration.properties', 'rb') as config_file:
            configurations.load(config_file)

        db_configs_dict = {}
        items_view = configurations.items()

        for item in items_view:
            db_configs_dict[item[0]] = item[1].data
        
        return db_configs_dict
    except:
        traceback.print_exc()
    finally:
        pass

def readRouteConfigurations():
    try:
        configurations = Properties()
        with open('configurations/RouteConfiguration.properties', 'rb') as config_file:
            configurations.load(config_file)

        db_configs_dict = {}
        items_view = configurations.items()

        for item in items_view:
            db_configs_dict[item[0]] = item[1].data
            
        return db_configs_dict
    except:
        traceback.print_exc()
    finally:
        pass