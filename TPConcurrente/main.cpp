#include "pcv.h"
#include "semaforo.h"
#include <iostream>
#include <thread>
#include <queue>
#include <vector>
#include <fstream>
#include <ctime>

Semaforo hay_espacio;
Semaforo hay_datos;
Semaforo hay_jobs;

//Escenario A
int trabajosTotales=100;
int cantJobsProductor=100;
int cantJobsConsumidor=50;
int cantPremium=10;

//Escenario B
/*int trabajosTotales=60;
int cantJobsProductor=20;
int cantJobsConsumidor=60;
int cantPremium=10;*/

//Escenario c
/*int trabajosTotales=1500;
int cantJobsProductor=500;
int cantJobsConsumidor=500;
int cantPremium=100;*/


int main() {

    init(hay_jobs, 0);
    init(hay_datos, 0);
    init(hay_espacio, 5);

    //Escenario A
    std::thread p(productor);
    std::thread a(asignador);
    std::thread c(consumidor);
    std::thread c2(consumidor);
    p.join();
    a.join();
    c.join();
    c2.join();


    //Escenario B
    /*std::thread p(productor);
    std::thread p2(productor);
    std::thread p3(productor);
    std::thread a(asignador);
    std::thread c(consumidor);
    p.join();
    p2.join();
    p3.join();
    a.join();
    c.join();*/

    //Escenario C
    /*std::thread p(productor);
    std::thread p2(productor);
    std::thread p3(productor);
    std::thread a(asignador);
    std::thread c(consumidor);
    std::thread c2(consumidor);
    std::thread c3(consumidor);
    p.join();
    p2.join();
    p3.join();
    a.join();
    c.join();
    c2.join();
    c3.join();*/



}
