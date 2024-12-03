from dicee import KGE, Execute
from dicee.config import Namespace
import pandas as pd
from dicee.static_funcs import get_er_vocab, get_re_vocab
from dicee.eval_static_funcs import evaluate_link_prediction_performance
from rdflib import Graph

data_path = "/home/sshivam/Work/dice-embeddings/KGs/commons_page_links_fr.ttl"

args = Namespace()
args.model = 'Keci'
args.optim = 'Adopt'
args.path_single_kg = data_path
args.num_epochs = 5
args.batch_size = 1024
args.lr = 0.1
args.embedding_dim = 32
args.input_dropout_rate = 0.0
args.hidden_dropout_rate = 0.0
args.feature_map_dropout_rate = 0.0
args.eval_model = 'train_val_test'
args.byte_pair_encoding = False
args.scoring_technique = 'NegSample'
args.trainer = 'PL'
args.backend = "rdflib"
result = Execute(args).start()

dformat = data_path[data_path.find(".") + 1 :]
train_triples = pd.DataFrame(data=[(str(s), str(p), str(o)) for s, p, o in Graph().parse(data_path)],
                                columns=['subject', 'relation', 'object'], dtype=str).values.tolist()
all_triples = train_triples
model = KGE(result["path_experiment_folder"])

evaluate_link_prediction_performance(model, triples=train_triples, er_vocab=get_er_vocab(all_triples), re_vocab=get_re_vocab(all_triples))