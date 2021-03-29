#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <ctime>
#include <random>
#include <sys/time.h>
#include <cstring>
#include "your_reduce.h"

// Extra
#include <iostream>   // std::cout
#include <string>     // std::string, std::to_string
#include <stdio.h>

#define MAX_LEN 268435456


// You may add your functions and variables here

void YOUR_Reduce(const int* sendbuf, int* recvbuf, int count) {
    /*
        Modify the code here.
        Your implementation should have the same result as this MPI_Reduce
        call. However, you MUST NOT use MPI_Reduce (or like) for your hand-in
        version. Instead, you should use MPI_Send and MPI_Recv (or like). See
        the homework instructions for more information.
    */
    //MPI_Reduce(sendbuf, recvbuf, count, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    /*
        You may assume:
        - Data type is always `int` (MPI_INT).
        - Operation is always MPI_SUM.
        - Process to hold final results is always process 0.
        - Number of processes is 2, 4, or 8.
        - Number of elements (`count`) is 1, 16, 256, 4096, 65536, 1048576,
          16777216, or 268435456.
        For other cases, your code is allowed to produce wrong results or even
        crash. It is totally fine.
    */
    
    int i;
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); //Get rank
    MPI_Comm_size(MPI_COMM_WORLD, &size); //Get size
    //printf("rank %d and size %d\n", rank, size);

    // Reset Buffer each run
    for(i=0; i<count; i++) {
        recvbuf[i] = 0;
    }

    if (size == 2){ //For 2 processors 
        if (rank != 0){
            MPI_Send(sendbuf, count, MPI_INT, 0, 0, MPI_COMM_WORLD);
            //printf("process %d sent sendbuf to process 0\n", rank);
        }
        else {
            int *buffer;
            buffer = (int*)malloc(MAX_LEN *sizeof(int));
            MPI_Recv(buffer, count, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("process 0 recieved sendbuf frm process 1\n");
            for(i=0; i<count; i++) {
                recvbuf[i] += sendbuf[i];
                recvbuf[i] += buffer[i];
            }
            //printf("Sucessfully added arrays\n");
        }
    }
    else if (size == 4){ //For 4 processors
        if (rank != 0 && rank %2 != 0){ // If rank is not zero and is a odd number
            int send_to = rank - 1;
            MPI_Send(sendbuf, count, MPI_INT, send_to, 0, MPI_COMM_WORLD); // Sending value to the processor rank -1
        }
        else if (rank %2 == 0){ //If rank is an even number
            int *buffer;
            buffer = (int*)malloc(MAX_LEN *sizeof(int));
            int recieve_from = rank + 1;
            MPI_Recv(buffer, count, MPI_DOUBLE, recieve_from, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("process 0 recieved sendbuf frm process 1\n");
            for(i=0; i<count; i++) {
                recvbuf[i] += sendbuf[i];
                recvbuf[i] += buffer[i];
            }
            //printf("Sucessfully added arrays\n");
            if (rank == 2){
                MPI_Send(recvbuf, count, MPI_INT, 0, 0, MPI_COMM_WORLD); // Sending value to the processor rank -1
            }
            else {
                int *buffer2;
                buffer2 = (int*)malloc(MAX_LEN *sizeof(int));
                MPI_Recv(buffer2, count, MPI_DOUBLE, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                for(i=0; i<count; i++) {
                    recvbuf[i] += buffer2[i];
                }
            }
        }
    }
    else { //For even number processors larger than 4
        if (rank != 0 && rank %2 != 0){ // If rank is not zero and is a odd number
            int send_to = rank - 1;
            MPI_Send(sendbuf, count, MPI_INT, send_to, 0, MPI_COMM_WORLD); // Sending value to the processor rank -1
        }
        else if (rank %2 == 0){ //If rank is an even number
            int *buffer;
            buffer = (int*)malloc(MAX_LEN *sizeof(int));
            int recieve_from = rank + 1;
            MPI_Recv(buffer, count, MPI_DOUBLE, recieve_from, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("process 0 recieved sendbuf frm process 1\n");
            for(i=0; i<count; i++) {
                recvbuf[i] += sendbuf[i];
                recvbuf[i] += buffer[i];
            }
        }
        if (rank % 2 == 0 && rank % 4 != 0){ //If rank is even and not a multiple of 4
            int send_to = rank - 2;
            MPI_Send(recvbuf, count, MPI_INT, send_to, 0, MPI_COMM_WORLD); // Sending value to the processor rank -2
        }
        else if (rank % 2 == 0 && rank % 4 == 0){ //If rank is even and is a multiple of 4
            int *buffer2;
            buffer2 = (int*)malloc(MAX_LEN *sizeof(int));
            int recieve_from = rank + 2;
            MPI_Recv(buffer2, count, MPI_DOUBLE, recieve_from, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(i=0; i<count; i++) {
                recvbuf[i] += buffer2[i];
            }
        }
        if (rank != 0 && rank % 4 == 0){ //If rank is not 0 and is a multiple of 4
            int send_to = rank - 4;
            MPI_Send(recvbuf, count, MPI_INT, send_to, 0, MPI_COMM_WORLD); // Sending value to the processor rank -4
        }
        else if (rank == 0){
            int *buffer3;
            buffer3 = (int*)malloc(MAX_LEN *sizeof(int));
            int recieve_from = rank + 4;
            MPI_Recv(buffer3, count, MPI_DOUBLE, recieve_from, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(i=0; i<count; i++) {
                recvbuf[i] += buffer3[i];
            }
        }
    }
}
