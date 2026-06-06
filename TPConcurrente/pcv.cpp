#include "pcv.h"
#include "semaforo.h"
#include <iostream>
#include <queue>
#include <mutex>
#include <thread>
#include <chrono>
#include <fstream>
#include <ctime>
#include <cstring>

extern int trabajosTotales;
extern int cantJobsProductor;
extern int cantJobsConsumidor;
extern int cantPremium;
int nroTrabajo=0;
int msSuperados=0;
std::queue<Job> freeQueue;
std::queue<Job> premiumQueue;
std::queue<Job> vram;
std::mutex mtx_crear;
std::mutex mtx_logs; //Este mutex es exclusivo para proteger las salidas
std::mutex mtx_queue;
std::mutex mtx_vram;
extern Semaforo hay_espacio;
extern Semaforo hay_datos;
extern Semaforo hay_jobs;

std::ofstream logFile("actividad.log");
time_t ahora = time(nullptr);



void productor(){
    for(int i=0; i<cantJobsProductor;i++){
        Job nuevo;

        //Cierro el mutex para evitar que se cree la cantidad de jobs premiums justa
        mtx_crear.lock();
        if(nroTrabajo<cantPremium){

            //Prioriza entrar aca para crear primero los jobs premium

            nuevo.nroSolicitud = nroTrabajo; //Le asigno al struct Job el numero de la tarea
            nroTrabajo++;
            mtx_crear.unlock();


            mtx_logs.lock();

            //std::cout<<"Creado el job numero: "<< nroTrabajo<<" de categoria premium"<<"\n";
             char* fecha = ctime(&ahora);
                fecha[strlen(fecha)-1] = '\0';
            logFile<< "[" <<  fecha << "]"<<"Creado el job numero: "<< nroTrabajo<<" de categoria premium"<<"\n";

            mtx_logs.unlock();

            nuevo.premium = true;



            std::this_thread::sleep_for(std::chrono::milliseconds(100));//Delay de 100ms para meter el job en la messagequeue


            mtx_queue.lock();  //Lock para cuidar los push y los pops de la messageQueue
            premiumQueue.push(nuevo); //Se encola en la queue de los premium
            mtx_queue.unlock();

            signal(hay_jobs); //Signal para avisar a el asignador que ya hay jobs para asignar en la Vram

            mtx_logs.lock();

                //std::cout<<"Job numero: "<< nuevo.nroSolicitud <<" encolado con exito"<<"\n";
                char* fecha2 = ctime(&ahora);
                fecha2[strlen(fecha2)-1] = '\0';
                logFile<< "[" << fecha2 << "]"<<"Job numero: "<< nuevo.nroSolicitud <<" encolado con exito"<<"\n";

            mtx_logs.unlock();


        }else{

            //En el caso de que no haya mas jobs premium, siempre va a entrar en el else para crear los jobs free


            nuevo.nroSolicitud = nroTrabajo; //Le asigno al struct Job el numero de la tarea
            nroTrabajo++;
            mtx_crear.unlock();
            nuevo.premium = false;

            mtx_logs.lock();

            //std::cout<<"Creado el job numero: "<< nroTrabajo<<" de categoria free"<<"\n";
            char* fecha = ctime(&ahora);
                fecha[strlen(fecha)-1] = '\0';
            logFile<< "[" << fecha << "]"<<"Creado el job numero: "<< nroTrabajo<<" de categoria free"<<"\n";

           mtx_logs.unlock();



            std::this_thread::sleep_for(std::chrono::milliseconds(100)); //Delay de 100ms para meter el job en la messagequeue

            mtx_queue.lock(); //Lock para cuidar los push y los pops de la messageQueue
            freeQueue.push(nuevo); //Se encola en la queue de los free
            mtx_queue.unlock();

            signal(hay_jobs);//Signal para avisar a el asignador que ya hay jobs para asignar en la Vram

            mtx_logs.lock();

            //std::cout<<"Job numero: "<< nuevo.nroSolicitud <<" encolado con exito"<<"\n";
            char* fecha2 = ctime(&ahora);
                fecha2[strlen(fecha2)-1] = '\0';
            logFile<< "[" << fecha2 << "]"<<"Job numero: "<< nuevo.nroSolicitud <<" encolado con exito"<<"\n";

            mtx_logs.unlock();
        }
    }
}

void asignador(){
    for(int i=0;i<trabajosTotales;i++){
        wait(hay_jobs); //Semaforo para evitar el busy waiting, no avanza hasta que un productor le mande un signal
        wait(hay_espacio); //Inicia en 5, por lo que inicialmente lo pasa, luego depende de los consumidores para saber si hay espacio en la Vram

        mtx_queue.lock();//Mismo lock utilizado en productor, su funcion es cuidar las queues

        if((!premiumQueue.empty() && msSuperados<8) || freeQueue.empty()){ //Analiza si pasaron a la vram mas de 8 jobs premium seguidos(superaria los 5000ms)
                                                                           // Tambien analiza si alguna cola esta vacia para saber de cual sacar
            Job asignar = premiumQueue.front();//Rescato el struct de la cola
            premiumQueue.pop();
            msSuperados++; //Va contando cuantos premiums seguidos van pasando
            mtx_queue.unlock();

            mtx_vram.lock(); //Este mutex cuida la queue de la Vram
            std::this_thread::sleep_for(std::chrono::milliseconds(450)); //Retardo de 450ms en asignar a la vram
            vram.push(asignar);//Se inserta en la vram
            signal(hay_datos); //signal del semaforo para que el consumidor sepa que hay datos para procesar
            mtx_vram.unlock();

            mtx_logs.lock();

            //std::cout<<"Job premium numero "<< asignar.nroSolicitud <<" asignado a la vram con exito" <<"\n";
            char* fecha = ctime(&ahora);
                fecha[strlen(fecha)-1] = '\0';
            logFile<< "[" << fecha << "]"<<"Job premium numero "<< asignar.nroSolicitud <<" asignado a la vram con exito" <<"\n";

            mtx_logs.unlock();
        }
        else{
            Job asignar = freeQueue.front(); //Lo mismo pero cambia la premium queue por la free queue
            freeQueue.pop();
            msSuperados=0; //Si pasa un free, el contador de premiums seguidos vuelve a cero
            mtx_queue.unlock();

            mtx_vram.lock();
            std::this_thread::sleep_for(std::chrono::milliseconds(450));
            vram.push(asignar);
            signal(hay_datos);
            mtx_vram.unlock();

            mtx_logs.lock();

            //std::cout<<"Job free numero "<< asignar.nroSolicitud <<" asignado a la vram con exito" <<"\n";
            char* fecha = ctime(&ahora);
                fecha[strlen(fecha)-1] = '\0';
            logFile<< "[" << fecha << "]"<<"Job free numero "<< asignar.nroSolicitud <<" asignado a la vram con exito" <<"\n";

            mtx_logs.unlock();
        }
    }
}

void consumidor(){
    for(int i=0; i<cantJobsConsumidor;i++){
        wait(hay_datos); //Espera a que haya datos para avanzar
        std::this_thread::sleep_for(std::chrono::milliseconds(600));//Delay de 600ms que el job debe estar en la vram

        mtx_vram.lock();
        Job procesando = vram.front();//Rescato el job de la vram
        std::this_thread::sleep_for(std::chrono::milliseconds(250));//Retardo de 250ms DENTRO del mutex, para forzar a que la espera sea exclusiva por job que sale de la vram
        vram.pop();
        signal(hay_espacio);//Signal a asignador para avisarle que se libero un espacio de la Vram
        mtx_vram.unlock();

        mtx_logs.lock();

         //std::cout<<"Se termino de procesar el job numero "<< procesando.nroSolicitud <<" con exito" <<"\n";
         char* fecha = ctime(&ahora);
                fecha[strlen(fecha)-1] = '\0';
         logFile<< "[" << fecha << "]"<<"Se termino de procesar el job numero "<< procesando.nroSolicitud <<" con exito" <<"\n";

        mtx_logs.unlock();
    }
}
