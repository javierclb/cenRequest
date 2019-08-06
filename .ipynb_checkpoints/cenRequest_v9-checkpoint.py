#!/usr/bin/env python
# coding: utf-8

import requests
import os
import pandas as pd
import json
import time
import datetime
from pandleau import *
import tableauserverclient as TSC
from os import listdir

class cenRequest:
    tokenDict={"awW26JzDOehlW1tU1EDz2vwo":{'xlimit':60,'delay':0},"XvKDYz9V3xgzMabdyc3NUAYt":{'xlimit':60,'delay':0},
               "sGxoh3rfOFj0WMpOn7CHszSh":{'xlimit':60,'delay':0},"L3TIxLFrr3uckMKUUDIuwwha":{'xlimit':60,'delay':0},
               "Iduxt1fHSPAUyv0ZOXAvRMYT":{'xlimit':60,'delay':0},'fL297twhGU3X3a83sXuXr84E':{'xlimit':60,'delay':0},
               "5ryBKY1IHAk6YQteCO624Vm1":{'xlimit':60,'delay':0},"cFebHbyPVPQhAtafoheR2PKX":{'xlimit':60,'delay':0}}
    tokenList=list(tokenDict.keys())
    lentl=len(tokenList)
    header={}
    rTable={}
    rSpec={}
    
    info={'barras':pd.DataFrame(),'empresas':pd.DataFrame(), 'centrales':pd.DataFrame(),     
          'tramos':pd.DataFrame(),'grupos':pd.DataFrame(),'giros':pd.DataFrame(),
          'lineas':pd.DataFrame()}
    
    def __init__(self,Res,start_token=0):
        self.setrTable()
        self.Res=Res
        self.rSpec=self.rTable[Res]
        self.chgToken(self.tokenList[start_token])
        print('Inicializando con token %s'%start_token)
        
    @classmethod
    def chgToken(cls,new):
        cls.token=new
        cls.header = {'authorization': "Token %s" %new}   
    
    @classmethod
    def setrTable(cls):
        if not bool(cls.rTable):
            rTable=pd.read_csv("tabla_recursos_v5.csv",sep=';', index_col=0 , encoding='latin-1')
            rTable=rTable.T
            rTable=rTable.to_dict()
            cls.rTable=rTable
    
    @classmethod
    def getInfo(cls,obj):
        url = "https://sipub.coordinador.cl/api/v2/recursos/infotecnica/"+obj+"/"
        querystring = {"limit":"300000","offset":"0"}
        infocen = requests.get(url, headers=cls.header, params=querystring)
        infocen_results=infocen.json().get("results")
        return infocen_results  
        
    @classmethod
    def Translate(cls,data,rec,infobar,databar,col=[]):
        if cls.info[rec].empty:
            cls.info[rec]=pd.DataFrame(cls.getInfo(rec))
            
        aux=cls.info[rec].copy()
        if col!=[]:
            aux=aux[col]
        salida=pd.merge(data,aux, how='left',left_on=databar,right_on=infobar)
        if rec=='barras':
            salida['nombre']=salida['nombre'].str.replace('BA S/E ','')
        return salida

    @classmethod
    def nextToken(cls,t0):
        t=t0
        while cls.tokenDict[cls.token]['xlimit']<4:
            t=(t+1)%cls.lentl
            cls.chgToken(cls.tokenList[t])
            if t==t0:
                print('Todos los tokens utilizados')
                return -1
        return t
    
    def sendRequest(self,date,limit=3000000000,offset=0):
        url = "https://sipub.coordinador.cl" + self.rSpec['URL']
        querystring = {self.rSpec['param']:date,"limit":str(limit),"offset":str(offset)}
        response = requests.get(url, headers=self.header, params=querystring) 
        try:
            xlim=int(response.headers['X-Rate-Limit-Remaining'])
        except KeyError:
            try:
                xlim=max(0,self.tokenDict[self.token]['xlimit']-1)
            except TypeError:
                pass
        self.tokenDict[self.token]['xlimit']=xlim
        return response
    
    def getResults(self,date,limit=3000000000,offset=0):
        response=self.sendRequest(date,limit,offset)
        sc=response.status_code
        if sc==200:
            return self.Code200(response,date)
        elif sc==429:
            token0=self.token
            t=self.Code429(response)
            if t >0:
                self.getResults(date,limit,offset)
            else:
                delay0=self.tokenDict[token0]['delay']
                self.wait(delay0)
                self.getResults(date,limit,offset)
        else:
            print('Codigo ',sc)
    
    def Code200(self,response,date):
        self.tokenDict[self.token]['delay']=0
        data=response.json().get("results")
        if data==[]:
            print('fecha %s vacia'%date)
        return data
    
    def Code429(self,response):
        self.tokenDict[self.token]['delay']=int(response.headers.get('Retry-After'))
        t0=self.tokenList.index(self.token)
        print('token %s saturado'%str(t0))
        t=self.nextToken(t0)
        return t
        
            
    def as_DF(self,data,expand=True): 
        df=pd.DataFrame(data)
        if not df.empty and expand==True:
            return self.expandData(df)
        else:
            return df
    
    def getDataFrame(self,date,expand=True):
        data=self.getResults(date)
        return self.as_DF(data,expand=expand)
    
    def expandData(self,df):
        #Revisar este if - para pasar de string a datetime
        if self.rSpec['param']=='fecha':
            if 'hora' in df.columns:
                df.fecha=pd.to_datetime(df.fecha)+pd.to_timedelta(df.hora,unit="h")+pd.DateOffset(hours=-1)
                df.drop(columns=['hora'],inplace=True)
            else:
                df.fecha=pd.to_datetime(df.fecha)
        else:
            if 'mes' in df.columns:
                df.mes=pd.to_datetime(df.mes)  
        
        return getattr(self,self.rTable[self.Res]['funcion'])(df)

    def afluentes(self,df):
        out=df
        out.rename(columns={'afluente':'afluente_promedio_diario[m3/s]'},inplace=True)
        return out
    
    def aguaCaida(self,df):
        out=df
        out.rename(columns={'agua_caida':'agua_caida[mm]','reservorio':'nombre_embalse'},inplace=True)
        return out
   
    def balanceSSCC(self,df):
        out=df
        out=self.Translate(df,'empresas','mnemotecnico','empresa_mnemotecnico',col=['mnemotecnico','nombre','grupo'])
        out=self.Translate(out,'grupos','id_infotecnica','grupo',col=['nombre','id_infotecnica'])
        out.rename(columns={'balance_neto':'balance_neto[CLP]','nombre_x':'nombre_empresa','nombre_y':'nombre_grupo'},inplace=True)
        out=out[['nombre_empresa','nombre_grupo','mes','balance_neto[CLP]']]
        return out

    def cmgProg(self,df):
        out=df
        tabla_llaves=pd.read_csv("llaves_CMGP.csv")
        out=pd.merge(df, tabla_llaves, on='llave_id')
        out['nombre_barra']=out['nombre'].str.replace('BA S/E ','')
        out.rename(columns={'costo':'cmg_programado[USD/MWh]'},inplace=True)
        out=out[['nombre_barra','fecha','cmg_programado[USD/MWh]']]
        return out
    
    def cmgKey(self,df):
        out=self.Translate(df,'barras','mnemotecnico','mnemotecnico_barra')
        out=out[['llave_id','llave_nombre_natural','mnemotecnico_barra', 'nombre']]
        return out
        
    def cmgReal(self,df):
        out=df
        out=self.Translate(df,'barras','mnemotecnico','barra_mnemotecnico')
        out.rename(columns={'nombre':'nombre_barra','costo_en_dolares':'cmg_real[USD/MWh]','costo_en_pesos':'cmg_real[CLP/kWh]'},inplace=True)
        out=out[['nombre_barra','fecha','cmg_real[USD/MWh]','cmg_real[CLP/kWh]']]
        return out
    
    def cmgEsperado(self,df):
        out=df
        out.rename(columns={'cmg_proyectado':'cmg_promedio_esperado[USD/MWh]'},inplace=True)
        return out
    
    def cotas(self,df):
        out=df
        out.rename(columns={'cota':'cota_embalse[msnm]','afluente_diario':'afluente_diario[m3/s]'},inplace=True)
        return out
    
    def dxReal(self,df):
        out=df
        out.rename(columns={'demanda':'demanda_real[MWh]'},inplace=True)
        return out
    
    def dxProg(self,df):
        out=df
        out.rename(columns={'demanda':'demanda_programada[MWh]'},inplace=True)
        return out
    
    def desviacion(self,df):
        out=df
        out.rename(columns={'generacion_programada':'generacion_programada[MW]','generacion_real':'generacion_real[MW]'},inplace=True)
        return df
    
    def gxReal(self,df):
        out=self.Translate(df,'centrales','id_infotecnica','id_central')
        out=self.Translate(out,'empresas','mnemotecnico','propietario',col=['mnemotecnico','nombre','grupo'])
        out=self.Translate(out,'grupos','id_infotecnica','grupo',col=['nombre','id_infotecnica'])
        out=out[['nombre_x','fecha','generacion', 'energia_ernc','tipo_central','descripcion','nombre_y','nombre']]
        out.rename(columns={'nombre_x':'nombre_central','nombre_y':'propietario','nombre':'grupo','generacion':'generacion[MW]','energia_ernc':'energia_ernc[MW]'},inplace=True)
        return out
    
    def potLinea(self,df):
        df.fecha=pd.to_datetime(df.fecha)+pd.to_timedelta(df.intervalos+1,unit="h")
        out=df
        out.linea_nombre=out.linea_nombre.astype(int)
        out=self.Translate(out,'lineas','id_infotecnica','linea_nombre')
        out=self.Translate(out,'empresas','mnemotecnico','propietario',col=['mnemotecnico','nombre'])
        out.drop(columns=['propietario'],inplace=True)
        out.rename(columns={'potencia':'potencia_transitada[MW]','nombre_x':'nombre_linea','nombre_y':'propietario','ssee':'ssee_referencia'},inplace=True)
        out=out[['nombre_linea','propietario','fecha','potencia_transitada[MW]','ssee_referencia']]
        return out
    
    def retiros(self,df):
        out=self.Translate(df,'barras','mnemotecnico','barra_mnemotecnico',col=['mnemotecnico','nombre'])
        out=self.Translate(out,'empresas','mnemotecnico','propietario_mnemotecnico',col=['mnemotecnico','nombre'])
        out.rename(columns={'nombre_x':'nombre_barra','nombre_y':'propietario_barra'},inplace=True)
        out=self.Translate(out,'empresas','mnemotecnico','suministrador_mnemotecnico',col=['mnemotecnico','nombre'])
        out=self.Translate(out,'empresas','mnemotecnico','cliente_mnemotecnico',col=['mnemotecnico','nombre'])
        out.rename(columns={'nombre_x':'suministrador','nombre_y':'cliente','retiro_ajustado':'retiro_ajustado[kWh]','retiro_ajustado_valorizado':'retiro_ajustado_valorizado[CLP]'},inplace=True)
        out=out[['nombre_barra','fecha', 'retiro_ajustado[kWh]','retiro_ajustado_valorizado[CLP]','cliente','suministrador','propietario_barra']]       
        return out
    
    def transf(self,df):
        campo={'TRANSFERENCIA_ENERGIA':('balance_energia','[kWh]'),'TRANSFERENCIA_POTENCIA':('balance_potencia','[kW]')}
        out=self.Translate(df,'empresas','mnemotecnico','propietario_mnemotecnico',col=['mnemotecnico','nombre'])
        c,s=campo[self.Res]
        out.rename(columns={c:c+s,'balance_valorizado':c+'[CLP]'},inplace=True)
        out=out[['nombre','mes',c+s,c+'[CLP]']]
        return out
   
    def default(self,df):
        return df
    
    def cargaM(self,oldDate,newDate):
        print('Empezando extraccion de datos a las %s'%datetime.datetime.now().time())
        Res=self.Res #recurso
        fr={'fecha':'D','mes':'MS','año':'Y'} #Tabla de frecuencia segun recurso
        dates=pd.date_range(start=oldDate, end=newDate, freq=fr[self.rSpec['param']]) #rango de fechas segun recurso
        token0=self.token
        t0=self.tokenList.index(token0)  #token inicial
        data=pd.DataFrame() #data inicial   
        i=0
        date=dates[i].strftime("%Y-%m-%d")
        while i<len(dates):
            response=self.sendRequest(date)
            sc=response.status_code
            if sc==200:
                result=self.Code200(response,date)
                ndata=self.as_DF(result)       #Aca es el unico lugar que se usa DataFrame
                data=pd.concat([data,ndata])
                i+=1
                try:
                    date=dates[i].strftime("%Y-%m-%d")
                except IndexError: pass
            elif sc==429:
                t=self.Code429(response)
                if t<0:
                    delay0=self.tokenDict[token0]['delay']
                    self.wait(delay0)
                    for tok in self.tokenDict:
                        self.tokenDict[tok]['delay']=0
                        self.tokenDict[tok]['xlimit']=60
            else:
                print('Codigo {}. Esperando 5 minutos'.format(sc))
                time.sleep(300)
        print('Proceso de extraccion termninado a las %s, con token %s'%(datetime.datetime.now().time(),self.tokenList.index(self.token)))
        return data
    
    def criticalDate(self,fDate,lDate,mode='update'): #modo alternativo :update
        fr={'fecha':'D','mes':'MS','año':'Y'}
        check={'fdate':lDate,'update':fDate}
        Mdate=check[mode]
        Mdat=self.getResults(Mdate)
        if Mdat==[]:
            return "Sin datos para el rango"
        dates=pd.date_range(start=fDate, end=lDate, freq=fr[self.rSpec['param']])
        inf=0
        sup=len(dates)-1
        while inf<sup:
            mid=int((sup+inf)/2)
            Mdate=dates[mid].strftime('%Y-%m-%d')
            Mdat=self.getResults(Mdate)
            if mid==inf: break
            if  Mdat==[] and mode=='fdate':
                inf=mid
            elif bool(Mdat) and mode=='update':
                inf=mid
            else:
                sup=mid
        if Mdat==[]:
            if mode=='fdate':
                Mdate=dates[mid+1].strftime('%Y-%m-%d')
            else:
                Mdate=dates[mid-1].strftime('%Y-%m-%d')
        return Mdate
    
    @staticmethod
    def wait(delay):
        zerotime=time.time()
        gap=int(time.time()-zerotime)
        sleep=max(delay-gap+5,0)
        retom=(datetime.datetime.now()+datetime.timedelta(seconds=(sleep))).time()
        print('Esperando %s, retomando a las %s' %(str(sleep),retom))
        time.sleep(sleep)
    
    @staticmethod
    def toTableau(data,outname,site_id="readeRiesgo",user='jdelafuente',password='javier1803',server='http://10.10.231.38:8500/',proj_name='CENAPI',mode='Overwrite'):
        if data.empty:
            print('Data vacia, no se sube')
            return    
        filename=outname+'.hyper'
        df_tableau = pandleau(data)
        df_tableau.to_tableau(filename, add_index=False)
        tableau_auth = TSC.TableauAuth(user, password,site_id=site_id)
        serv = TSC.Server(server)

        with serv.auth.sign_in(tableau_auth):
            all_projects, pagination_item = serv.projects.get()
            project=[proj.name for proj in all_projects]
            INDEX=project.index(proj_name)
            new_datasource = TSC.DatasourceItem(all_projects[INDEX].id)
            #try:
            serv.datasources.publish(new_datasource, filename,mode, connection_credentials=None)
            #except:
             #   serv.datasources.publish(new_datasource, filename,'CreateNew', connection_credentials=None)
        
        path=os.listdir('.')
        for item in path:
            if item.endswith(".log") or item.startswith('hyper') or item.endswith(".hyper"):
                os.remove(item)




