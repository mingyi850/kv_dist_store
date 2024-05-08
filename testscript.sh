#! /bin/bash


ROUNDS=1000
PUTS=1
KEYS=5
NODES=20

GETS=${GETS:-"1"}
# REP_FACTOR=${REP_FACTOR:-"1 3 5 7 9"}
REP_FACTOR=${REP_FACTOR:-"9"}
# R_QUORUM=${R_QUORUM:-"1 2 3 4 5 6 7 8 9"}
# W_QUORUM=${W_QUORUM:-"1 2 3 4 5 6 7 8 9"}
R_QUORUM=${R_QUORUM:-"1"}
W_QUORUM=${W_QUORUM:-"1"}
# CLIENTS=${CLIENTS:-"1 2 3"}
CLIENTS=${CLIENTS:-"3"}
# DELAY=${DELAY:-"0 2"}
DELAY=${DELAY:-"2"}
# DROP=${DROP:-"0 5 10"}
DROP=${DROP:-"5"}
# DOWN_UP=${DOWN_UP:-"0-0 1-30 2-40 5-60"}
DOWN_UP=${DOWN_UP:-"1-30"}

# Run the mix test with a combination of all these factors
for gets in $GETS; do
    for rep_factor in $REP_FACTOR; do
        if [ $rep_factor -gt $nodes ]; then 
            continue
        fi
        for r_quorum in $R_QUORUM; do
            #Check that quorum is less than or equal to rep_factor
            if [ $r_quorum -gt $rep_factor ]; then
                continue
            fi
            for w_quorum in $W_QUORUM; do
                if [ $w_quorum -gt $rep_factor ]; then
                    continue
                fi
                for clients in $CLIENTS; do
                    for delay in $DELAY; do
                        for drop in $DROP; do
                            for down_up in $DOWN_UP; do
                                down_N_up=$(echo $down_up | tr '-' ' ')
                                echo "Running test with rounds: $ROUNDS, gets: $gets, puts: $PUTS, keys: $KEYS, rep_factor: $rep_factor, r_quorum: $r_quorum, w_quorum: $w_quorum, nodes: $NODES, clients: $clients, delay: $delay, drop rate: $drop, down/up prob: $down_N_up"
                                mix test test/test_cases_args.exs $ROUNDS $gets $PUTS $KEYS $rep_factor $r_quorum $w_quorum $NODES $clients $delay $drop $down_N_up
                            done
                        done
                    done
                done
            done
        done
    done
done

