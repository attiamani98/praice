import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder


def get_model():
    numeric_features = ["hours_until_perished"]
    categorical_features = ["product"]
    
    categorical_pipeline = Pipeline([
        ("categorial_imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
        ("ordinal_encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])

    numeric_pipeline = Pipeline([
        ("numeric_imputer", SimpleImputer()),
    ])

    model = Pipeline([
        ("column_transformer", ColumnTransformer([
            ("numeric_pipeline", numeric_pipeline, numeric_features),
            ("categorical_pipeline", categorical_pipeline, categorical_features),
        ]).set_output(transform="pandas")),
        ("model", Ridge()),
    ])
    
    model.name = __name__
    model.param_grid = {
                              "model__max_depth": [10, 20, 30],
                              "model__l2_regularization": [0, 5, 10],
                          },

    return model