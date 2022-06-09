import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Principal {
    public static void main(String[] args) {
        try{
            File f=new File("operaciones.bin");
            FileInputStream fis=new FileInputStream(f);
            DataInputStream dis=new DataInputStream(fis);
            Class.forName("com.mysql.jdbc.Driver");
            String url="jdbc:mysql://localhost/Vuelo";
            String user="root";
            String password="";
            Connection con=DriverManager.getConnection(url, user, password);
            Statement st=con.createStatement();
            String cadena="insert into Vuelos values(?,?,?,?)";
            PreparedStatement ps1=con.prepareStatement(cadena);
            cadena="insert into Ciudades values(?,?,?,?)";
            PreparedStatement ps2=con.prepareStatement(cadena);
            cadena="delete from vuelos where numero=?";
            PreparedStatement ps3=con.prepareStatement(cadena);
            cadena="delete from Ciudades where ciudad=?";
            PreparedStatement ps4=con.prepareStatement(cadena);
            cadena="update Vuelos set numero=?,fecha=? where id=?";
            PreparedStatement ps5=con.prepareStatement(cadena);
            cadena="update Ciudades set ciudad=?,pais=? where id=?";
            PreparedStatement ps6=con.prepareStatement(cadena);
            cadena="select * from vuelos where id=? and numero=?";
            PreparedStatement ps7=con.prepareStatement(cadena);
            cadena="select id from ciudades where ciudad=?";
            PreparedStatement ps8=con.prepareStatement(cadena);
            cadena="select destino from vuelos where id=?";
            PreparedStatement ps9=con.prepareStatement(cadena);
            ResultSet rs,rs1,rs2,rs3;
            
            System.out.println("---ANTES DE ACTUALIZAR----");
            
            rs=st.executeQuery("select * from Ciudades");
            while(rs.next()){
              System.out.println(rs.getInt(1)+" "+rs.getString(2)+" "+rs.getString(3)+" "+rs.getString(4));
            }
            System.out.println("-----------------------------------------------");
            rs=st.executeQuery("select * from Vuelos");
            while(rs.next()){
              System.out.println(rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getInt(3)+" "+rs.getString(4));
            }
            
             System.out.println("-----------------------------------------------");
            int id,num,destino;
            String ciudad,fecha;
            char op;
            
            try{
                while(true){
                  id=dis.readInt();
                  num=dis.readInt();
                  ciudad=dis.readUTF();
                  fecha=dis.readUTF();
                  op=dis.readChar();
                  
                  System.out.println(id+" "+num+" "+ciudad+" "+fecha+" "+op);
                  
                  if(op=='A'){
                  ps7.setInt(1,id);
                  ps7.setInt(2, num);
                  rs1=ps7.executeQuery();
                  if(rs1.next())
                  System.out.println("No se puede dar de alta pues ya existe");
                  else{
                  ps8.setString(1,ciudad);
                  rs2=ps8.executeQuery();
                  if(rs2.next())
                  destino=rs2.getInt(1);
                  else{
                  rs=st.executeQuery("select max(id) from ciudades");
                  rs.next();
                  destino=rs.getInt(1)+10;
                  ps2.setInt(1, destino);
                  ps2.setString(2,ciudad);
                  ps2.setString(3, "pais");
                  ps2.setString(4,"continente");
                  ps2.executeUpdate();
                  }
                  ps1.setInt(1, id);
                  ps1.setInt(2, num);
                  ps1.setInt(3,destino);
                  ps1.setString(4,fecha);
                  ps1.executeUpdate();
                  }
                  
                  } else if(op=='B'){
                      ps4.setString(1,ciudad);
                      if(ps4.executeUpdate()==0)
                          System.out.println("Esa ciudad no existie en la base de datos");
                  }else{
                     ps9.setInt(1, id);
                     rs3=ps9.executeQuery();
                     if(rs3.next()){
                        ps6.setString(1, ciudad);
                        ps6.setString(2, "pais");
                        ps6.setInt(3, rs3.getInt(1));
                        ps6.executeUpdate();
                        ps5.setInt(1, num);
                        ps5.setString(2, fecha);
                        ps5.setInt(3, id);
                        ps5.executeUpdate();
                        
                    }else System.out.println("No se puede actualizar pues no existe");   
              }
             }    
            }catch(EOFException ex){}
            
            dis.close();
            fis.close();
            
            System.out.println("---DESPUES DE ACTUALIZAR----");
            
             rs=st.executeQuery("select * from Ciudades");
            while(rs.next()){
              System.out.println(rs.getInt(1)+" "+rs.getString(2)+" "+rs.getString(3)+" "+rs.getString(4));
            }
             System.out.println("-----------------------------------------------");
            rs=st.executeQuery("select * from Vuelos");
            while(rs.next()){
              System.out.println(rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getInt(3)+" "+rs.getString(4));
            }
            
            
            
            st.close();
            ps1.close();
            ps2.close();
            ps3.close();
            ps4.close();
            ps5.close();
            ps6.close();
            ps7.close();
            ps8.close();
            ps9.close();
            
            con.close();
                   
        }catch(ClassNotFoundException ex){
            System.out.println(ex);
        }catch(SQLException ex){
           System.out.println(ex); 
        }catch(IOException ex){
          System.out.println(ex);   
        }
    }
  
}