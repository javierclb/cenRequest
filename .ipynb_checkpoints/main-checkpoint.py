#!/usr/bin/env python3

from sys import argv, exit
from cenRequest_v9 import cenRequest
import pandas as pd
import datetime

def cargaDiaria():
    tableauRes=pd.read_csv('tableauRes.csv', sep=';',index_col=0).T
    tableauRes=tableauRes.to_dict('records')[0]
    st=0
    for Res in tableauRes.keys():
        last_update=tableauRes[Res]
        print('\nInicializando con recurso: %s, ultima fecha actualizada: %s'%(Res,last_update))
        R=cenRequest(Res,start_token=st)
        today=datetime.date.today().strftime("%Y-%m-%d")
        yesterday =(datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
        ndate=R.criticalDate(last_update,yesterday)
        print('Ultima fecha con datos: ',ndate)
        if ndate!=last_update and ndate!="Sin datos para el rango":
            plusday =(pd.to_datetime(last_update) + datetime.timedelta(1)).strftime("%Y-%m-%d")
            print('\nProcediento a cargar datos desde %s a %s'%(plusday,ndate))
            data=R.cargaM(plusday,ndate)
            R.toTableau(data,Res,mode='Append',proj_name='CENAPI2')
            tableauRes[Res]=ndate
        else:
            print('\nNo existen actualizaciones')
        st=R.tokenList.index(R.token)
        overwrite=pd.DataFrame(tableauRes, index=['Ultima fecha actualizada']).T
        overwrite.to_csv('tableauRes.csv',sep=';')

def cargaMasiva(oldDate='2018-01-01'):
    R=cenRequest('COSTOS_MARGINALES_REALES')
    Rlist=list(R.rTable.keys())
    lis=[0,1,9,11,15,18,22,24,27,36,2,12,43,44]
    newDate=(datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
    for p in lis:
        res=Rlist[p]
        print('\nRecurso: ',res,'\n')
        t=R.tokenList.index(R.token) 
        R=cenRequest(res,start_token=t)
        data=R.cargaM(oldDate,newDate)
        cenRequest.toTableau(data,res,proj_name='CENAPI2')
        print('\n')

def main():
    res='COSTO_MARGINAL_PROGRAMADO'
    print('\nRecurso: ',res,'\n') 
    R=cenRequest(res)
    data=R.cargaM('2018-01-01','2019-04-09')
    cenRequest.toTableau(data,res,proj_name='CENAPI2')
    print('\n')
 
if __name__=='__main__': 
    main()
 
