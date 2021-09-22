//Jan Chlebek, PUT, Jan 2021
//compile: gcc -o out -pthread concurrent.c; Linux only
//task: multithreaded program that performs a parallel calculation of the number of characters and lines in all ordinary files located in the subdirectories of the directory specified by the first argument of the program call and having one of the extensions given as further arguments.
//my test run on Ubuntu WSL: ./out /mnt/c/Users/Chlebek/Documents/GitHub/PSW/files txt xlsx
//expected output: Process finished successfully.\nFound %d lines and %d characters.\n

#define _XOPEN_SOURCE 500
#include <ctype.h>
#include <fcntl.h>
#include <ftw.h>
#include <obstack.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>

#define debug 1     //additional printf
#define N 100       //max stack size -1
#define rlimit 100  //finder recursion limit
int g_argc;         //accepted file formats in global variable
char **g_argv;

//stage end control
int sigendff = 0;  //file finder completion signal
int sigendproc;    //lower bound for finder cond signal

//stacks safety control
pthread_mutex_t pstackmutex, cstackmutex;
pthread_cond_t pstackunderflowcnd, cstackunderflowcnd;  //stack underflow protection
pthread_cond_t pstackoverflowcnd, cstackoverflowcnd;    //stack overflow protection

//proc stack - collecting file paths
char *pstack[N];
int ppointer = -1;

//counter stack - collecting proc results
struct Calc {
    int lines;
    int characters;
};

struct Calc cstack[N];
int cpointer = -1;

//file finder section
int finder(const char *file, const struct stat *statptr, int fflag, struct FTW *pfwt) {
    if (fflag == FTW_F) {  //isfile
        if (access(file, R_OK) != 0) {
            if (debug)
                printf("%s not accessible, skipped.\n", file);
        } else
            for (int i = 2; g_argc > i; i++) {
                const char *dot = strrchr(file, '.');  //get file extension
                if (dot != NULL) {
                    dot++;
                    if (strcmp(dot, g_argv[i]) == 0) {  // is accepted file extension
                        //push to file stack
                        pthread_mutex_lock(&pstackmutex);
                        if (ppointer == N - 1)
                            pthread_cond_wait(&pstackoverflowcnd, &pstackmutex);
                        ppointer++;
                        pstack[ppointer] = strdup(file);
                        if (ppointer <= sigendproc)
                            pthread_cond_signal(&pstackunderflowcnd);
                        pthread_mutex_unlock(&pstackmutex);
                        if (debug)
                            printf("%s pushed\n", file);
                    }
                } else if (debug)
                    printf("%s has no extension, skipped.\n", file);
            }
    }
    return 0;
}

void *file_finder(void *info) {
    const char *startpath = g_argv[1];
    int ret = nftw(startpath, finder, rlimit, FTW_DEPTH | FTW_MOUNT);  //recursive search

    //signal of finder completion
    pthread_mutex_lock(&pstackmutex);
    sigendff = 1;
    pthread_cond_broadcast(&pstackunderflowcnd);
    pthread_mutex_unlock(&pstackmutex);
    return NULL;
}

//proc section
void *proc(void *info) {
    const char *path;
    while (1) {  //pop from file stack
        pthread_mutex_lock(&pstackmutex);
        while (ppointer < 0) {
            if (sigendff == 1) {  //end if file finder finished
                if (debug)
                    printf("Proc finished\n");
                pthread_mutex_lock(&cstackmutex);
                sigendproc--;
                if (sigendproc == 0)
                    pthread_cond_signal(&cstackunderflowcnd);
                pthread_mutex_unlock(&cstackmutex);
                pthread_mutex_unlock(&pstackmutex);
                return NULL;
            }
            pthread_cond_wait(&pstackunderflowcnd, &pstackmutex);
        }
        if (ppointer == N - 1)
            pthread_cond_signal(&pstackoverflowcnd);
        path = pstack[ppointer];
        ppointer--;
        pthread_mutex_unlock(&pstackmutex);

        //read file and count data
        struct Calc calc = {0, 0};
        unsigned char *f;
        struct stat sb;
        int fd = open(path, O_RDONLY);
        fstat(fd, &sb);
        f = (char *)mmap(0, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        for (int i = 0; i < sb.st_size; i++) {
            if (f[i] == '\n')
                calc.lines++;
            calc.characters += !isspace(f[i]);
        }
        printf("%s  Lines: %d, non-whitespace characters: %d\n", path, calc.lines, calc.characters);

        //push to counter stack
        pthread_mutex_lock(&cstackmutex);
        if (cpointer == N - 1)
            pthread_cond_wait(&cstackoverflowcnd, &cstackmutex);
        cpointer++;
        cstack[cpointer] = calc;
        if (cpointer == 0)
            pthread_cond_signal(&cstackunderflowcnd);
        pthread_mutex_unlock(&cstackmutex);
    }
}

//counter section
struct Calc result = {0, 0};
void *counter(void *info) {
    while (1) {  //pop from counter stack and calculate result
        pthread_mutex_lock(&cstackmutex);
        while (cpointer < 0) {
            if (sigendproc == 0) {
                if (debug)
                    printf("Calc finished\n");
                pthread_mutex_unlock(&cstackmutex);
                return NULL;
            }
            pthread_cond_wait(&cstackunderflowcnd, &cstackmutex);
        }
        result.lines += cstack[cpointer].lines;
        result.characters += cstack[cpointer].characters;
        if (cpointer == N - 1)
            pthread_cond_signal(&cstackoverflowcnd);
        cpointer--;
        pthread_mutex_unlock(&cstackmutex);
    }
}

int main(int argc, char **argv) {
    g_argc = argc;
    g_argv = argv;

    if (access(g_argv[1], R_OK) != 0) {
        printf("%s not accessible, process terminated.\n", g_argv[1]);
        return 1;
    }

    pthread_mutex_init(&pstackmutex, NULL);
    pthread_mutex_init(&cstackmutex, NULL);
    pthread_cond_init(&pstackunderflowcnd, NULL);
    pthread_cond_init(&pstackoverflowcnd, NULL);
    pthread_cond_init(&cstackoverflowcnd, NULL);

    if (debug)
        printf("Avalibe processes: %d\n", get_nprocs());
    sigendproc = get_nprocs() - 1;
    pthread_t prc[sigendproc + 1];

    pthread_create(&prc[1], NULL, file_finder, NULL);  //start file finder
    for (int i = 2; get_nprocs() > i; i++)             //start procs
        pthread_create(&prc[i], NULL, proc, NULL);
    pthread_create(&prc[0], NULL, counter, NULL);  //start counter
    pthread_join(prc[1], NULL);                    //wait for end of file finder
    pthread_create(&prc[1], NULL, proc, NULL);     //use file finder as additional proc
    for (int i = 1; get_nprocs() > i; i++)         //wait for end of procs
        pthread_join(prc[i], NULL);
    pthread_join(prc[0], NULL);  //wait for end of counter

    printf("Process finished successfully.\nFound %d lines and %d characters.\n", result.lines, result.characters);
    return 0;
}