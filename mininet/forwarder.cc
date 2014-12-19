#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
using namespace std;

int main(int argc, char *argv[]){
    if(argc != 3){
        printf("usage: %s port dst_ip\n", argv[0]);
        exit(0);
    }
    int port = atoi(argv[1]);
    char *dst_ip = argv[2];
    
    int recv_sock, send_sock;
    struct sockaddr_in recv_addr, dest_addr;
    memset(&recv_addr, 0, sizeof(recv_addr));
    memset(&dest_addr, 0, sizeof(dest_addr));
    recv_addr.sin_family = AF_INET;
    recv_addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    recv_addr.sin_port = htons(port);
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_addr.s_addr = inet_addr(dst_ip);
    dest_addr.sin_port = htons(port);

    if((recv_sock = socket(PF_INET, SOCK_DGRAM, 0)) == -1){
        printf("create socket error\n");
        exit(0);
    }
    if(bind(recv_sock, (struct sockaddr*)&recv_addr, sizeof(recv_addr)) == -1){
        printf("bind socket error\n");
        exit(0);
    }
    if((send_sock = socket(PF_INET, SOCK_DGRAM, 0)) == -1){
        printf("create socket error\n");
        exit(0);
    }
    char buff[4096];
    int len;
    while(1){
        len = recvfrom(recv_sock, buff, 4096, 0, NULL, NULL);
        if(len > 0){
            buff[len] = '\0';
            sendto(send_sock, buff, len, 0, (struct sockaddr*)&dest_addr, sizeof(dest_addr));
        }
    }
    return 0;
}
