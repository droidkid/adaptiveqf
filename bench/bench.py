from sacred import Experiment
import os

ex = Experiment()

@ex.config
def filter_config():
    quotient_bits=20
    remainder_bits=9
    num_queries=1000000

@ex.capture
def run_filter_bench(quotient_bits, remainder_bits, num_queries):
    os.system('./bench/run_test.sh %s %s %s' % (quotient_bits, remainder_bits, num_queries))


@ex.automain
def run_experiment():
    run_filter_bench()
    ex.add_artifact('output.txt')
    ex.add_artifact('unif_q.csv')
    ex.add_artifact('unif_i.csv')
    pass


