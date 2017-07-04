from datetime import date

import numpy as np
import typing

import sqlite3


class Feature(np.ndarray):

    def __new__(cls, input_data, feature_making_fn: typing.Callable=None):
        """

        :param input_data: Input data to be stored in the Feature, this can a Numpy ndarray, a single number that
        numpy.array() can convert, or any other form of data that is is to be coerced by the feature_making_fn.
        :param feature_making_fn: Optional function to call on the input_data to turn it into an nd_arra
        :return:
        """
        # After https: // docs.scipy.org / doc / numpy / user / basics.subclassing.html

        if feature_making_fn is not None:
            fn_out  = feature_making_fn(input_data)
            feature_vector = fn_out if isinstance(fn_out, np.ndarray) else np.array(fn_out)

            assert isinstance(feature_vector, np.ndarray), 'feature_making_fn did not return numpy.ndarray'
        elif input_data is not np.ndarray:
            feature_vector = np.array(input_data)
        else:
            feature_vector = input_data

        obj = np.asarray(feature_vector).view(cls)

        # add the new attributes to the created instance
        obj.input_data = input_data
        obj.feature_making_function = feature_making_fn

        # Finally return the newly created object:
        return obj

    def __array_finalize__(self, obj):
        if obj is None:
            return
        self.input_data = getattr(obj, 'input_data', None)
        self.input_to_feat_map_fn = getattr(obj, 'input_to_feat_map_fn', None)

    def magnitude(self):
        return np.linalg.norm(self)


class FeatureModel(Feature):

    def __new__(cls, input_data, id: str, feature_model_making_fn: typing.Callable=None, good_data: bool = None, bad_data_reason: str = None):
        obj = super().__new__(cls, input_data=input_data, feature_making_fn=feature_model_making_fn)
        obj.id = id
        obj.good_data = good_data
        obj.bad_data_reason = bad_data_reason
        return obj

    def __array_finalize__(self, obj):
        if obj is None:
            return
        self.id = getattr(obj, 'id', None)
        self.id = getattr(obj, 'good_data', None)
        self.id = getattr(obj, 'bad_data_reason', None)

    @staticmethod
    def create_models_for_all_teams(model_making_fn: typing.Callable, entities) -> dict:

        return {ent: model_making_fn(ent) for ent in entities}


class FootballMatchPredictor(object):
    def __init__(self, models: {str: FeatureModel},
                 home_advantage_boost:float = 0.0,
                 decision_threshold:float = 0.0
                 ):

        self.models = models
        self.threshold = decision_threshold
        self.home_advantage = home_advantage_boost

    def predict(self, home_team: str, away_team:str) -> (str, float, str):

        home_model = self.models[home_team]
        away_model = self.models[away_team]
        home_advantage_offset = self.home_advantage
        threshold = self.threshold

        assert isinstance(home_model, FeatureModel)
        assert isinstance(away_model, FeatureModel)

        assert home_model.shape == away_model.shape, 'Cannot predict, models have different dimensions'



        home_metric = home_advantage_offset + home_model
        try:
            home_metric = home_advantage_offset + home_model[0]
        except IndexError:
            pass # Intentionally ignore  these

        # See if the other, i.e. away_team, has a different feature model for away matches, if so then we'll use that.
        away_metric = away_model  # Default, is to expect to just have a single model for the team
        try:
            away_metric = away_model[1]
        except IndexError:
            pass # Intentionally ignore it, we'll assume we've got a common model for home and away


        distance = home_metric - away_metric

        predicted_result = 'draw'
        if home_metric > away_metric and abs(distance) > threshold:
            predicted_result = 'home_win'
        elif home_metric < away_metric and abs(distance) > threshold:
            predicted_result = 'away_win'


        if home_model.good_data is None and away_model.good_data is None:
            return predicted_result, distance, None
        elif home_model.good_data is False or away_model.good_data is False:
            bad_data_explanation = ''
            prefix = ''
            for model in [home_model, away_model]:
                if model.good_data is False:
                    why = 'No reason given' if model.bad_data_reason is None else model.bad_data_reason
                    model_blurb = 'Bad model for %s - %s' % (model.id, why)
                    bad_data_explanation += prefix
                    bad_data_explanation +=  model_blurb
                    prefix = ', '

            return predicted_result, distance, bad_data_explanation
        else:
            return predicted_result, distance, None


class FeatureModelRanking(object):
    def __init__(self, input_data: [],
                 feature_making_fn: typing.Callable,
                 id_fn: typing.Callable =None):

        unsorted_feature_models = [FeatureModel(data, id=id_fn(data), feature_model_making_fn=feature_making_fn) for
                                   data in input_data]

        self.feature_models = sorted(unsorted_feature_models, reverse=True)

        self.id2feature_models = {feature.id: feature for feature in self.feature_models}

        self.id2ranking = {}
        self.gen_id2rankings()

    def gen_id2rankings(self):
        table_pos = 0
        team_place = 0
        last_feature = None

        for feature in self.feature_models:
            table_pos += 1
            if last_feature is None or feature < last_feature:
                # Cope with first place then allow for subsequent tied positions by only incrementing the team's place
                # in the league if their performance was worse than the previous team.
                team_place = table_pos

            self.id2ranking[feature.id] = team_place
            last_feature = feature

    def __iter__(self):
        for id in self.id2ranking:
            yield self.id2ranking[id], self.id2feature_models[id].input_data

    def __str__(self) -> str:
        return '%s' % list(self)
