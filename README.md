# Generate dataset for training a stock prediction model
This will generate 2 *.db data files in ./data, which the ./dataset/dataset class will use to load data.

## Usage
- clone this repository:

```
$ git clone https://github.com/pauljhp/SA_dataset_creation
```

- Then run:
```
$ bash ./setup.sh
```

You should also put a config.json file under ./FinancialModelingPrep/.config/, otherwise the API will keep prompting you for API key.

After properly setting up, either run:

```
$ bash run.sh
```

with default settings;

or

```
$ python3 create_dataset.py -i <INDEXNAME> -s <START DATE (YYYY-MM-DD)> -p <PRINT EVERY ITER>
```