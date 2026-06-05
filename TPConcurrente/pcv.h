#ifndef PCV_H_INCLUDED
#define PCV_H_INCLUDED

struct Job{
    int nroSolicitud;
    bool premium;
};

void productor();
void asignador();
void consumidor();

#endif // PCV_H_INCLUDED
