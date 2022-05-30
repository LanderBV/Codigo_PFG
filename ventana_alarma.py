#### Imports
from tkinter import *
import tkinter.messagebox 
from datetime import datetime, timedelta
import sqlite3

#### Ruta del archivo
DATABASE_LOCATION = "/home/instalador/airflow-docker/dags/Datos_ETL/temporal.sqlite"

#### Clase para crear tablas en las ventanas emergentes
class Table:
    def __init__(self, ventana, datos):
        self.ventana = ventana
        self.datos = datos
        self.col = len(datos[0])
        self.fil = len(datos)
        

    def crear(self):
        for i in range(self.fil):
            for j in range(self.col):
                if i ==0:
                    self.entry = Entry(self.ventana, width=20, bg='LightSteelBlue',fg='Black',
                                       font=('Arial', 16, 'bold'))
                else:
                    self.entry = Entry(self.ventana, width=20, fg='blue',
                               font=('Arial', 16, ''))

                self.entry.grid(row=i, column=j)
                self.entry.insert(END, self.datos[i][j])


def fechas():
        fecha_hoy = datetime.today()
        dias_atras = timedelta(days=7) # Cuantos días se quiere retroceder
        fecha_select = fecha_hoy - dias_atras # Última semana
        fecha_select = fecha_select.strftime("%Y-%m-%d %H:%M:%S")
        return datetime.strptime(fecha_select,"%Y-%m-%d %H:%M:%S")

   
#### Funcionalidad de la ventana
# Ventana temporal Bilbao
def Bilbao(): 
    datos=[("ID", "Ciudad", "Temperatura Min ºC", "Temperatura Max ºC", "Fecha")]
    fecha_select =  fechas() # Fechas para filtrar el select

    con = sqlite3.connect(DATABASE_LOCATION)
    cur = con.cursor()
    data = cur.execute("SELECT * FROM temporal WHERE localizacion='Bilbao' AND fecha>?", (fecha_select,))
    for row in data:
        datos.append(row)

    v_Bilbao = tkinter.Tk() 
    v_Bilbao.title("Temporal en Bilbao") 
    v_Bilbao.resizable(width=0, height=0)
    table1 = Table(v_Bilbao, datos)
    table1.crear()
    v_Bilbao.mainloop()
  

# Ventana temporal Donostia
def Donostia(): 
    datos=[("ID", "Ciudad", "Temperatura Min ºC", "Temperatura Max ºC", "Fecha")]
    fecha_select =  fechas() # Fechas para filtrar el select

    con = sqlite3.connect(DATABASE_LOCATION)
    cur = con.cursor()
    data = cur.execute("SELECT * FROM temporal WHERE localizacion='Donostia/San Sebasti\u00e1n' AND fecha>?", (fecha_select,))
    for row in data:
        datos.append(row)

    v_Donostia = tkinter.Tk() 
    v_Donostia.title("Temporal en Donostia") 
    v_Donostia.resizable(width=0, height=0)
    table2 = Table(v_Donostia, datos)
    table2.crear()
    v_Donostia.mainloop()
   
    

# Ventana temporal Vitoria
def Vitoria(): 
    datos=[("ID", "Ciudad", "Temperatura Min ºC", "Temperatura Max ºC", "Fecha")]
    fecha_select =  fechas() # Fechas para filtrar el select

    con = sqlite3.connect(DATABASE_LOCATION)
    cur = con.cursor()
    data = cur.execute("SELECT * FROM temporal WHERE localizacion='Vitoria-Gasteiz' AND fecha>?", (fecha_select,))
    for row in data:
        datos.append(row)

    v_Vitoria = tkinter.Tk() 
    v_Vitoria.title("Temporal en Vitoria-Gasteiz") 
    v_Vitoria.resizable(width=0, height=0)
    table3 = Table(v_Vitoria, datos)
    table3.crear()
    v_Vitoria.mainloop()


# Base de la ventana
ventana = tkinter.Tk() 
ventana.title("¡Grieta Detectada!") 
ventana.resizable(width=0, height=0)
ventana.geometry('600x300') 
  
# Label y botones
Etiqueta = Label(ventana, text = "Se ha detectado una grieta en el dispositivo.\n A continuación puede ver el temporal de las diferenes ciudades del Pais Vasco.")
Button1 = Button(ventana, text = "Bilbao", command = lambda: Bilbao(), pady = 10)
Button2 = Button(ventana, text = "Donostia", command = lambda: Donostia(), pady = 10) 
Button3 = Button(ventana, text = "Vitoria-Gasteiz", command = lambda: Vitoria(), pady = 10) 
Warning_img= Label(ventana, image = "::tk::icons::warning")

Etiqueta.place(bordermode = OUTSIDE, width = 530, x = 35,y= 80)
Button1.place(bordermode = OUTSIDE,height = 40, width = 150, x = 30,y = 200) 
Button2.place(bordermode = OUTSIDE,height = 40, width = 150, x = 225,y = 200) 
Button3.place(bordermode = OUTSIDE,height = 40, width = 150, x = 420,y = 200) 
Warning_img.place(bordermode = OUTSIDE, width = 50, height = 50, x = 275,y= 10)

# Loop principal  
ventana.mainloop()