import sys
import os
from path_utils import setup_paths

# Setup paths and get CORE_DIR
CORE_DIR = setup_paths()
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers.pipelines.token_classification import TokenClassificationPipeline
from datetime import datetime

model_checkpoint = "Davlan/bert-base-multilingual-cased-ner-hrl"
tokenizer = AutoTokenizer.from_pretrained(model_checkpoint)
model = AutoModelForTokenClassification.from_pretrained(model_checkpoint)

class TokenClassificationChunkPipeline(TokenClassificationPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, sentence, offset_mapping=None, **preprocess_params):
        tokenizer_params = preprocess_params.pop("tokenizer_params", {})
        truncation = True if self.tokenizer.model_max_length and self.tokenizer.model_max_length > 0 else False
        inputs = self.tokenizer(
            sentence,
            return_tensors="pt",
            truncation=True,
            return_special_tokens_mask=True,
            return_offsets_mapping=True,
            return_overflowing_tokens=True,  # Return multiple chunks
            max_length=self.tokenizer.model_max_length,
            padding=True
        )
        #inputs.pop("overflow_to_sample_mapping", None)
        num_chunks = len(inputs["input_ids"])

        for i in range(num_chunks):
            if self.framework == "tf":
                model_inputs = {k: tf.expand_dims(v[i], 0) for k, v in inputs.items()}
            else:
                model_inputs = {k: v[i].unsqueeze(0) for k, v in inputs.items()}
            if offset_mapping is not None:
                model_inputs["offset_mapping"] = offset_mapping
            model_inputs["sentence"] = sentence if i == 0 else None
            model_inputs["is_last"] = i == num_chunks - 1
            yield model_inputs

    def _forward(self, model_inputs):
        # Forward
        special_tokens_mask = model_inputs.pop("special_tokens_mask")
        offset_mapping = model_inputs.pop("offset_mapping", None)
        sentence = model_inputs.pop("sentence")
        is_last = model_inputs.pop("is_last")

        overflow_to_sample_mapping = model_inputs.pop("overflow_to_sample_mapping")

        output = self.model(**model_inputs)
        logits = output["logits"] if isinstance(output, dict) else output[0]


        model_outputs = {
            "logits": logits,
            "special_tokens_mask": special_tokens_mask,
            "offset_mapping": offset_mapping,
            "sentence": sentence,
            "overflow_to_sample_mapping": overflow_to_sample_mapping,
            "is_last": is_last,
            **model_inputs,
        }

        # We reshape outputs to fit with the postprocess inputs
        model_outputs["input_ids"] = torch.reshape(model_outputs["input_ids"], (1, -1))
        model_outputs["token_type_ids"] = torch.reshape(model_outputs["token_type_ids"], (1, -1))
        model_outputs["attention_mask"] = torch.reshape(model_outputs["attention_mask"], (1, -1))
        model_outputs["special_tokens_mask"] = torch.reshape(model_outputs["special_tokens_mask"], (1, -1))
        model_outputs["offset_mapping"] = torch.reshape(model_outputs["offset_mapping"], (1, -1, 2))

        return model_outputs

# Function to anonymise a sentence
def anonymise_sentence(sentence, ignore_list=None):
    # Ensure there's an initial value for anonymised_sentence
    anonymised_sentence = sentence
    ignore_list = [word.lower() for word in ignore_list] if ignore_list else []
    ents = pipe(sentence)
    offset = 0  # Track the offset caused by replacements

    for ent in ents:
        if ent["word"].lower() in ignore_list:
            continue
        start, end = ent["start"], ent["end"]
        entity = ent["word"]
        tag = f"[{ent['entity_group']}]"

        # Replace entity with tag considering the offset
        anonymised_sentence = anonymised_sentence[:start + offset] + tag + anonymised_sentence[end + offset:]
        offset += len(tag) - (end - start)  # Update offset for next replacement

    return anonymised_sentence



def create_ignore_list():

    mystical_words = pd.read_csv('data/raw/mystical_words/mysticality_dict.csv')
    mystical_words = mystical_words['Final Lexicon'].tolist()
    mystical_words = [subword for word in mystical_words for subword in word.split('/')]
    mystical_words = [word.lower() for word in mystical_words]

    ignore_list = ['bot', 'boti']
    ignore_list.extend(mystical_words)

    return ignore_list


def identify_new_journals(dfj, dfj_processed):

    # Get all JournalUniqueID from dfj_processed
    journal_ids = dfj_processed['JournalUniqueID'].tolist()

    return journal_ids


if __name__ == '__main__':
    
    # Set to True to include mystical words in ignore list
    use_mystical_words = False

    pipe = TokenClassificationChunkPipeline(model=model, tokenizer=tokenizer, aggregation_strategy="simple")
    
    dfj = pd.read_csv(os.path.join(CORE_DIR, 'data/processed/2_journals_preprocessed.csv'))
    anon_filepath = os.path.join(CORE_DIR, 'data/processed/4_journals_anon_content_both.csv')

    ignore_list = create_ignore_list() if use_mystical_words else ['bot', 'boti']
    counter = 0 

    if os.path.exists(anon_filepath):
        dfj_processed = pd.read_csv(anon_filepath)
        journal_ids = identify_new_journals(dfj, dfj_processed)
        # Filter to include those not in journal_ids
        dfj = dfj[~dfj['JournalUniqueID'].isin(journal_ids)]
        print(f"Processing {len(dfj)} new rows.")


    for index, row in dfj.iterrows():
        counter += 1
        sentence = row['Content']
        try:
            # print(f"Original: {sentence}")
            anonymised = anonymise_sentence(sentence, ignore_list)
            # print(f"Anonymised: {anonymised}")
            dfj.at[index, 'JournalAnonymised'] = anonymised
            if counter % 10 == 0:
                print(f"Processed {counter} rows out of {len(dfj)}.")
        except Exception as e:
            print(f"Error processing row {index}. Error: {e}")
            continue

    # Concat the new rows to the existing dfj_processed
    if os.path.exists(anon_filepath):
        dfj_processed = pd.concat([dfj_processed, dfj], ignore_index=True)
        # Sort by JournalTimestamp
        dfj_processed = dfj_processed.sort_values(by='Timestamp')
        # reset index
        dfj_processed = dfj_processed.reset_index(drop=True)
        # if col starts with Unnamed, drop it
        dfj_processed = dfj_processed.loc[:, ~dfj_processed.columns.str.contains('^Unnamed')]
        # Export file
        dfj_processed.to_csv(anon_filepath)
        print(f"File exported to {anon_filepath}.")

    # Remove non-anon col
    dfj_full_anon = dfj_processed.drop(columns=['Content'])
    output_filepath = os.path.join(CORE_DIR, 'data/processed/5_journals_anon_content_only.csv')
    dfj_full_anon.to_csv(output_filepath)
    print(f"File exported to {output_filepath}.")        