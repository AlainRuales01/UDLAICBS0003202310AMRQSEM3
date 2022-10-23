from datetime import date, datetime


def join_strings(string1,string2):
    return f'{string1} {string2}'

def obt_gender(gen):
    if gen == 'M':
        return 'MASCULINO'
    elif gen == 'F':
        return 'FEMENINO'
    else:
        return 'NO DEFINIDO'

def obt_date(date_string):
    modified_date =  datetime.strptime(date_string,'%d-%b-%y')
    return modified_date
