/*
 * Jonathan Jensen
 * 10/29/2015
 * Task 3 problem 3
 * CS 486
 */

/*
**personal note
**I compiled using gcc -I/usr/include/postgresql/ -L/usr/lib/postgresql/9.3/lib/ -o task3 task3.c -lpq

*/
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

int
main(int argc, char **argv)
{
    const char *conninfo;
    PGconn     *conn;
    PGresult   *res;
    PGresult   *res2;
    int         nFields;
    int         i,
                j;

    /*
     * If the user supplies a parameter on the command line, use it as the
     * conninfo string; otherwise default to setting dbname=postgres and using
     * environment variables or defaults for all other connection parameters.
     */
    if (argc > 1)
        conninfo = argv[1];
    else
        conninfo = "host=dbclass.cs.pdx.edu port=5432 dbname=f15ddb47 user=f15ddb47 password=**********";

    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    /*
     * Our test case here involves using a cursor, for which we must be inside
     * a transaction block.  We could do the whole thing with a single
     * PQexec() of "select * from pg_database", but that's too trivial to make
     * a good example.
     */

    /* Start a transaction block */
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    /*
     * Should PQclear PGresult whenever it is no longer needed to avoid memory
     * leaks
     */
    PQclear(res);

    /*
     * Fetch rows from pg_database, the system catalog of databases
     */
    res = PQexec(conn, "DECLARE agentcursor CURSOR FOR select agent_id, salary from agent order by salary");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    
    //create new *agent_raise* table
    res = PQexec(conn, "CREATE TABLE agent_raise (agent_id integer, raise integer)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "CREATE TABLE failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    res = PQexec(conn, "FETCH ALL in agentcursor");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    //compute *raise*
    int curr_id = 0;
    int curr_salary = 0;
    int prev_salary = 0;
    int next_salary = 0;
    int ntuples = PQntuples(res);
    int raise = 0;
    int avg = 0;
    char buffer [256];
    
    curr_id = atoi(PQgetvalue(res, 0, 0));
    curr_salary = atoi(PQgetvalue(res, 0, 1));
    next_salary = atoi(PQgetvalue(res, 1, 1));

    // For agent with lowest salary.
    // He/She doesnt get to be in the for loop
        avg = (curr_salary + next_salary)/2;
        //if current employee deserves a raise....
        if(curr_salary < avg){
            //print insert when raise is deserved
            /*
            printf("%-15s", "insert");
            printf("\n");
            */

            raise = (avg - curr_salary);

            sprintf(buffer, "INSERT INTO agent_raise (agent_id, raise) VALUES (%d, %d)", curr_id, raise);
            res2 = PQexec(conn, buffer);
            //res2 = PQexec(conn, "INSERT INTO agentraise (id, aRaise) values (%d, %d)", currid, raise );
            if (PQresultStatus(res2) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
                PQclear(res2);
                exit_nicely(conn);
            }
        }


        
 
    //for all rows in agentcursor 
    for(i = 1; i < ntuples-1; i++){
        //PQgetvalue returns a cString. atoi converts it to int
        curr_id  = atoi(PQgetvalue(res, i, 0));
        curr_salary = atoi(PQgetvalue(res, i, 1));
        prev_salary = atoi(PQgetvalue(res, i-1, 1));
        next_salary = atoi(PQgetvalue(res, i+1, 1)); 
       
        //error checking
        /* 
        printf("%-15d", curr_id);
        printf("\n");
        printf("%-15d", curr_salary);
        printf("\n");
        printf("%-15d", prev_salary);
        printf("\n");
        printf("%-15d", next_salary);
        printf("\n");
        */
        
        
        avg = (prev_salary + next_salary)/2; 
        //if current employee deserves a raise....
        if(curr_salary < avg){
            //print insert when raise is deserved
            /*
            printf("%-15s", "insert");
            printf("\n");
            */

            raise = (avg - curr_salary);

            sprintf(buffer, "INSERT INTO agent_raise (agent_id, raise) VALUES (%d, %d)", curr_id, raise);
            res2 = PQexec(conn, buffer);           
            //res2 = PQexec(conn, "INSERT INTO agentraise (id, aRaise) values (%d, %d)", currid, raise );
            if (PQresultStatus(res2) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
                PQclear(res2);
                exit_nicely(conn);
            }
        }
        
    
    }
    
    PQclear(res2);

    res = PQexec(conn, "CLOSE agentcursor");
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    PQclear(res);
    
    
    
    /* Start a transaction block to display the new agent_raise table */
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    /*
     * Should PQclear PGresult whenever it is no longer needed to avoid memory
     * leaks
     */
    PQclear(res);

    /*
     * Fetch rows from pg_database, the system catalog of databases
     */
    res = PQexec(conn, "DECLARE raisecursor CURSOR FOR select * from agent_raise");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

    res = PQexec(conn, "FETCH ALL in raisecursor");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    /* first, print out the attribute names */
    
    nFields = PQnfields(res);
    for (i = 0; i < nFields; i++)
        printf("%-15s", PQfname(res, i));
    printf("\n\n");
    

    /* next, print out the rows */
    
    for (i = 0; i < PQntuples(res); i++)
    {
        for (j = 0; j < nFields; j++)
            printf("%-15s", PQgetvalue(res, i, j));
        printf("\n");
    }

    

    
    PQclear(res);

    /* close the portal ... we don't bother to check for errors ... */
    res = PQexec(conn, "CLOSE raisecursor");
    PQclear(res);


    /* end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);

    /* close the connection to the database and cleanup */
    PQfinish(conn);

    return 0;
}
