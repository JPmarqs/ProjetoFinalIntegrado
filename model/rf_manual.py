#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
PIPELINE BINÁRIO - PREVISÃO DE ACIDENTES COM VÍTIMAS (PRF)
IMPLEMENTAÇÃO DO ZERO (HARD-CODED)
=============================================================================
Todos os componentes de ML são implementados do zero:
  - Decision Tree (Gini, corte otimizado)
  - Random Forest (bootstrap + subamostragem de features)
  - SMOTE (k-NN + interpolação sintética)
  - Métricas: Accuracy, Precision, Recall, F1, AUC-ROC, Balanced Accuracy
  - Train/Test Split estratificado
  - Cross-Validation estratificada
  - Feature Importance (Gini importance)
Normalização:MinMaxScaler (scikit-learn permitido conforme solicitado)
=============================================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import cv2
import os
import time
import warnings
warnings.filterwarnings('ignore')


# =============================================================================
# 0. IMPLEMENTAÇÃO DO ZERO: ÁRVORE DE DECISÃO
# =============================================================================

class NodeDecisionTree:
    """Nó de uma árvore de decisão."""
    __slots__ = ('feature_idx', 'threshold', 'left', 'right',
                 'value', 'is_leaf', 'samples', 'gini', 'n_samples')

    def __init__(self, *, feature_idx=None, threshold=None,
                 left=None, right=None, value=None,
                 is_leaf=False, samples=0, gini=0.0, n_samples=0):
        self.feature_idx = feature_idx
        self.threshold = threshold
        self.left = left
        self.right = right
        self.value = value           # classe majoritária (leaf)
        self.is_leaf = is_leaf
        self.samples = samples       # distribuição de classes no nó
        self.gini = gini
        self.n_samples = n_samples


class DecisionTreeClassifierFromScratch:
    """
    Árvore de Decisão para classificação binária/multi-classe.

    Parâmetros
    ----------
    max_depth : int ou None
        Profundidade máxima da árvore.
    min_samples_split : int
        Número mínimo de amostras para dividir um nó interno.
    min_samples_leaf : int
        Número mínimo de amostras em uma folha.
    max_features : int ou None
        Número de features a considerar ao procurar o melhor split.
        Se None, usa todas. Usado pelo Random Forest.
    random_state : int ou None
        Seed para reprodutibilidade.
    """

    def __init__(self, max_depth=None, min_samples_split=2,
                 min_samples_leaf=1, max_features=None, random_state=None):
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.min_samples_leaf = min_samples_leaf
        self.max_features = max_features
        self.random_state = random_state
        self.root = None
        self.n_classes = 0
        self.classes = None
        self.n_features = 0

    # -----------------------------------------------------------------
    # Cálculo de Gini
    # -----------------------------------------------------------------
    @staticmethod
    def _gini(y):
        """Calcula o índice de Gini de um vetor de labels."""
        n = len(y)
        if n == 0:
            return 0.0
        _, counts = np.unique(y, return_counts=True)
        probs = counts / n
        return 1.0 - np.sum(probs ** 2)

    @staticmethod
    def _gini_split(y_left, y_right):
        """Gini ponderado de um split."""
        n_total = len(y_left) + len(y_right)
        if n_total == 0:
            return 0.0
        gini_left = DecisionTreeClassifierFromScratch._gini(y_left)
        gini_right = DecisionTreeClassifierFromScratch._gini(y_right)
        return (len(y_left) / n_total) * gini_left + \
               (len(y_right) / n_total) * gini_right

    # -----------------------------------------------------------------
    # Melhor split para uma feature contínua
    # -----------------------------------------------------------------
    def _best_split_for_feature(self, X_col, y, sorted_indices):
        """Encontra o melhor threshold para uma feature contínua."""
        best_gini = float('inf')
        best_threshold = None
        best_left_idx = None
        best_right_idx = None

        n = len(y)
        sorted_y = y[sorted_indices]
        sorted_X = X_col[sorted_indices]

        # Únicos valores candidatos (meio entre valores consecutivos)
        unique_vals = np.unique(sorted_X)
        if len(unique_vals) < 2:
            return None, None, None, float('inf')

        # Gera thresholds como pontos médios entre valores consecutivos
        thresholds = (unique_vals[:-1] + unique_vals[1:]) / 2.0

        for thr in thresholds:
            left_mask = sorted_X <= thr
            right_mask = ~left_mask

            y_left = sorted_y[left_mask]
            y_right = sorted_y[right_mask]

            # Verifica restrição de min_samples_leaf
            if (len(y_left) < self.min_samples_leaf or
                    len(y_right) < self.min_samples_leaf):
                continue

            gini = self._gini_split(y_left, y_right)
            if gini < best_gini:
                best_gini = gini
                best_threshold = thr
                left_indices = np.where(left_mask)[0]
                right_indices = np.where(right_mask)[0]
                best_left_idx = sorted_indices[left_indices]
                best_right_idx = sorted_indices[right_indices]

        return best_left_idx, best_right_idx, best_threshold, best_gini

    # -----------------------------------------------------------------
    # Construção recursiva da árvore
    # -----------------------------------------------------------------
    def _build(self, X, y, depth):
        """Constrói a árvore recursivamente.

        X e y são subarrays (fatias do dataset original) com os mesmos
        primeiros eixos. Isso garante que ``sorted_indices`` produzidos
        por ``np.argsort`` sejam sempre válidos para indexar ``y``.
        """
        n_samples = len(y)
        classes, counts = np.unique(y, return_counts=True)
        majority_class = classes[np.argmax(counts)]

        gini = self._gini(y)

        # Condições de parada
        stop = False
        if self.max_depth is not None and depth >= self.max_depth:
            stop = True
        elif n_samples < self.min_samples_split:
            stop = True
        elif len(classes) == 1:
            stop = True

        if stop:
            return NodeDecisionTree(
                value=majority_class, is_leaf=True,
                samples=dict(zip(classes.tolist(), counts.tolist())),
                gini=gini, n_samples=n_samples
            )

        # Selecionar subconjunto de features (para Random Forest)
        n_features_total = X.shape[1]
        if self.max_features is not None and self.max_features < n_features_total:
            rng = np.random.RandomState(self.random_state)
            feature_indices = rng.choice(
                n_features_total, size=self.max_features, replace=False
            )
        else:
            feature_indices = np.arange(n_features_total)

        best_gini = float('inf')
        best_feature = None
        best_threshold = None
        best_left_mask = None
        best_right_mask = None

        for feat_idx in feature_indices:
            X_col = X[:, feat_idx]
            sorted_indices = np.argsort(X_col)

            left_idx, right_idx, thr, gini_split = \
                self._best_split_for_feature(X_col, y, sorted_indices)

            if left_idx is not None and gini_split < best_gini:
                best_gini = gini_split
                best_feature = feat_idx
                best_threshold = thr
                # Converter índices absolutos em máscaras booleanas locais
                left_mask = np.zeros(n_samples, dtype=bool)
                left_mask[left_idx] = True
                right_mask = ~left_mask
                best_left_mask = left_mask
                best_right_mask = right_mask

        # Se não encontrou split melhor que o nó atual → folha
        if best_left_mask is None:
            return NodeDecisionTree(
                value=majority_class, is_leaf=True,
                samples=dict(zip(classes.tolist(), counts.tolist())),
                gini=gini, n_samples=n_samples
            )

        # Recursão — passamos subarrays para manter X e y alinhados
        left_child = self._build(X[best_left_mask], y[best_left_mask], depth + 1)
        right_child = self._build(X[best_right_mask], y[best_right_mask], depth + 1)

        return NodeDecisionTree(
            feature_idx=best_feature, threshold=best_threshold,
            left=left_child, right=right_child,
            value=majority_class,
            samples=dict(zip(classes.tolist(), counts.tolist())),
            gini=gini, n_samples=n_samples
        )

    def fit(self, X, y):
        """Treina a árvore."""
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y)
        self.classes = np.unique(y)
        self.n_classes = len(self.classes)
        self.n_features = X.shape[1]
        self.root = self._build(X, y, depth=0)
        return self

    # -----------------------------------------------------------------
    # Predição
    # -----------------------------------------------------------------
    def _predict_one(self, x, node):
        """Prediz um único sample percorrendo a árvore."""
        if node.is_leaf:
            return node.value
        if x[node.feature_idx] <= node.threshold:
            return self._predict_one(x, node.left)
        else:
            return self._predict_one(x, node.right)

    def predict(self, X):
        """Prediz classes para múltiplos samples."""
        X = np.asarray(X, dtype=np.float64)
        return np.array([self._predict_one(x, self.root) for x in X])

    # -----------------------------------------------------------------
    # Probabilidades (proporção de classes nas folhas)
    # -----------------------------------------------------------------
    def _predict_proba_one(self, x, node):
        if node.is_leaf:
            total = sum(node.samples.values())
            proba = np.zeros(self.n_classes)
            for i, c in enumerate(self.classes):
                proba[i] = node.samples.get(c, 0) / total
            return proba
        if x[node.feature_idx] <= node.threshold:
            return self._predict_proba_one(x, node.left)
        else:
            return self._predict_proba_one(x, node.right)

    def predict_proba(self, X):
        """Prediz probabilidades para múltiplos samples."""
        X = np.asarray(X, dtype=np.float64)
        return np.vstack([self._predict_proba_one(x, self.root) for x in X])

    # -----------------------------------------------------------------
    # Feature Importance (Gini importance acumulada)
    # -----------------------------------------------------------------
    def _collect_importance(self, node, importances, total_samples):
        """Coleta importância recursivamente."""
        if node.is_leaf:
            return
        # Redução de impureza ponderada pelo número de samples
        n_left = node.left.n_samples
        n_right = node.right.n_samples
        n_total = node.n_samples
        if n_total == 0:
            return
        reduction = (n_total / total_samples * node.gini
                     - (n_left / n_total * node.left.gini
                        + n_right / n_total * node.right.gini))
        importances[node.feature_idx] += max(reduction, 0)
        self._collect_importance(node.left, importances, total_samples)
        self._collect_importance(node.right, importances, total_samples)

    def feature_importances(self):
        """Retorna a importância de cada feature."""
        importances = np.zeros(self.n_features)
        total_samples = self.root.n_samples
        self._collect_importance(self.root, importances, total_samples)
        total = importances.sum()
        if total > 0:
            importances /= total
        return importances


# =============================================================================
# 1. IMPLEMENTAÇÃO DO ZERO: RANDOM FOREST
# =============================================================================

class RandomForestClassifierFromScratch:
    """
    Random Forest para classificação binária/multi-classe.

    Parâmetros
    ----------
    n_estimators : int
        Número de árvores na floresta.
    max_depth : int ou None
        Profundidade máxima de cada árvore.
    min_samples_split : int
        Número mínimo de amostras para dividir um nó.
    min_samples_leaf : int
        Número mínimo de amostras em uma folha.
    max_features : str ou int ou float
        Número de features para cada árvore.
        'sqrt' → sqrt(n_features), 'log2' → log2(n_features),
        int → valor absoluto, float → fração.
    random_state : int ou None
        Seed para reprodutibilidade.
    n_jobs : int
        (Ignorado em implementação sequencial, mantido para interface.)
    """

    def __init__(self, n_estimators=100, max_depth=None,
                 min_samples_split=2, min_samples_leaf=1,
                 max_features='sqrt', random_state=None, n_jobs=-1):
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.min_samples_leaf = min_samples_leaf
        self.max_features = max_features
        self.random_state = random_state
        self.n_jobs = n_jobs
        self.trees = []
        self.feature_indices_per_tree = []
        self.classes = None
        self.n_features = 0
        self._feature_importances = None

    def _resolve_max_features(self, n_features):
        """Resolve o parâmetro max_features para um inteiro."""
        if isinstance(self.max_features, int):
            return min(self.max_features, n_features)
        elif isinstance(self.max_features, float):
            return max(1, int(self.max_features * n_features))
        elif self.max_features == 'sqrt':
            return max(1, int(np.sqrt(n_features)))
        elif self.max_features == 'log2':
            return max(1, int(np.log2(n_features)))
        else:
            return n_features

    def fit(self, X, y):
        """Treina a Random Forest com bootstrap sampling."""
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y)
        self.classes = np.unique(y)
        n_samples, self.n_features = X.shape
        max_feat = self._resolve_max_features(self.n_features)

        self.trees = []
        self.feature_indices_per_tree = []

        for i in range(self.n_estimators):
            # Criar rng para esta árvore (reprodutível)
            tree_rng = np.random.RandomState(
                self.random_state + i if self.random_state is not None else None
            )

            # Bootstrap sample
            bootstrap_idx = tree_rng.choice(
                n_samples, size=n_samples, replace=True
            )
            X_bootstrap = X[bootstrap_idx]
            y_bootstrap = y[bootstrap_idx]

            # Seleção aleatória de features
            feature_idx = tree_rng.choice(
                self.n_features, size=max_feat, replace=False
            )
            self.feature_indices_per_tree.append(feature_idx)

            X_sub = X_bootstrap[:, feature_idx]

            # Treinar árvore
            tree = DecisionTreeClassifierFromScratch(
                max_depth=self.max_depth,
                min_samples_split=self.min_samples_split,
                min_samples_leaf=self.min_samples_leaf,
                max_features=None,  # já fizemos a seleção manualmente
                random_state=self.random_state + i if self.random_state is not None else None
            )
            tree.fit(X_sub, y_bootstrap)
            self.trees.append(tree)

        # Calcular feature importances
        self._compute_feature_importances()
        return self

    def _compute_feature_importances(self):
        """Calcula a importância média das features (Gini importance)."""
        importances = np.zeros(self.n_features)
        for tree, feat_idx in zip(self.trees, self.feature_indices_per_tree):
            tree_imp = tree.feature_importances()
            for j, fi in enumerate(feat_idx):
                importances[fi] += tree_imp[j]
        total = importances.sum()
        if total > 0:
            importances /= total
        self._feature_importances = importances

    @property
    def feature_importances_(self):
        """Retorna a importância das features."""
        if self._feature_importances is None:
            return np.zeros(self.n_features)
        return self._feature_importances

    def predict(self, X):
        """Prediz classes (voto majoritário)."""
        X = np.asarray(X, dtype=np.float64)
        all_preds = np.array([
            tree.predict(X[:, fi])
            for tree, fi in zip(self.trees, self.feature_indices_per_tree)
        ])  # shape: (n_trees, n_samples)

        # Voto majoritário
        result = np.zeros(X.shape[0], dtype=all_preds.dtype)
        for i in range(X.shape[0]):
            votes = all_preds[:, i]
            unique, counts = np.unique(votes, return_counts=True)
            result[i] = unique[np.argmax(counts)]
        return result

    def predict_proba(self, X):
        """Prediz probabilidades (média das probabilidades das árvores)."""
        X = np.asarray(X, dtype=np.float64)
        n_classes = len(self.classes)
        proba_sum = np.zeros((X.shape[0], n_classes))

        for tree, feat_idx in zip(self.trees, self.feature_indices_per_tree):
            proba_sum += tree.predict_proba(X[:, feat_idx])

        return proba_sum / len(self.trees)


# =============================================================================
# 2. IMPLEMENTAÇÃO DO ZERO: SMOTE
# =============================================================================

class SMOTEFromScratch:
    """
    SMOTE (Synthetic Minority Over-sampling Technique) do zero.

    Parâmetros
    ----------
    k_neighbors : int
        Número de vizinhos mais próximos para gerar amostras sintéticas.
    random_state : int ou None
        Seed para reprodutibilidade.
    sampling_strategy : float
        Proporção desejada da classe minoritária em relação à majoritária.
    """

    def __init__(self, k_neighbors=5, random_state=None, sampling_strategy=1.0):
        self.k_neighbors = k_neighbors
        self.random_state = random_state
        self.sampling_strategy = sampling_strategy

    def _euclidean_distances(self, A, B):
        """Calcula distâncias euclidianas entre A (n, d) e B (m, d)."""
        # ||a - b||^2 = ||a||^2 + ||b||^2 - 2*a.b
        A_sq = np.sum(A ** 2, axis=1)[:, np.newaxis]  # (n, 1)
        B_sq = np.sum(B ** 2, axis=1)[np.newaxis, :]   # (1, m)
        dist_sq = A_sq + B_sq - 2.0 * A @ B.T
        dist_sq = np.maximum(dist_sq, 0)  # evita negativos por erro numérico
        return np.sqrt(dist_sq)

    def _k_nearest_neighbors(self, X_minority):
        """Retorna os k-vizinhos mais próximos para cada ponto."""
        n = len(X_minority)
        k = min(self.k_neighbors, n - 1)
        dists = self._euclidean_distances(X_minority, X_minority)
        # Zera a diagonal (distância a si mesmo = infinito)
        np.fill_diagonal(dists, np.inf)
        # Ordena e pega os k menores
        knn_indices = np.argsort(dists, axis=1)[:, :k]
        return knn_indices

    def fit_resample(self, X, y):
        """
        Gera amostras sintéticas para a classe minoritária.

        Parameters
        ----------
        X : ndarray de shape (n_samples, n_features)
        y : ndarray de shape (n_samples,)

        Returns
        -------
        X_resampled, y_resampled
        """
        X = np.asarray(X, dtype=np.float64)
        y = np.asarray(y)

        rng = np.random.RandomState(self.random_state)

        classes, counts = np.unique(y, return_counts=True)
        class_counts = dict(zip(classes.tolist(), counts.tolist()))

        # Classe majoritária e minoritária
        majority_class = max(class_counts, key=class_counts.get)
        minority_class = min(class_counts, key=class_counts.get)

        n_majority = class_counts[majority_class]
        n_minority = class_counts[minority_class]

        # Quantos gerar
        n_synthetic = int(n_majority * self.sampling_strategy) - n_minority
        if n_synthetic <= 0:
            return X.copy(), y.copy()

        X_minority = X[y == minority_class]

        # Encontrar k-vizinhos
        knn_indices = self._k_nearest_neighbors(X_minority)

        X_synthetic = np.zeros((n_synthetic, X.shape[1]))
        for i in range(n_synthetic):
            # Escolher aleatoriamente um ponto da classe minoritária
            idx = rng.randint(0, len(X_minority))
            # Escolher aleatoriamente um dos k-vizinhos
            nn_idx = rng.randint(0, knn_indices.shape[1])
            neighbor = X_minority[knn_indices[idx, nn_idx]]

            # Interpolar
            alpha = rng.random()
            X_synthetic[i] = X_minority[idx] + alpha * (neighbor - X_minority[idx])

        # Concatenar
        X_resampled = np.vstack([X, X_synthetic])
        y_resampled = np.concatenate([y, np.full(n_synthetic, minority_class)])

        # Embaralhar
        shuffle_idx = rng.permutation(len(y_resampled))
        return X_resampled[shuffle_idx], y_resampled[shuffle_idx]


# =============================================================================
# 3. IMPLEMENTAÇÃO DO ZERO: MÉTRICAS
# =============================================================================

def accuracy_score_scratch(y_true, y_pred):
    """Calcula a acurácia."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    return np.mean(y_true == y_pred)


def precision_score_scratch(y_true, y_pred):
    """Calcula a precisão (para classe positiva = 1)."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    vp = np.sum((y_true == 1) & (y_pred == 1))
    fp = np.sum((y_true == 0) & (y_pred == 1))
    return vp / (vp + fp) if (vp + fp) > 0 else 0.0


def recall_score_scratch(y_true, y_pred):
    """Calcula o recall (sensibilidade) (para classe positiva = 1)."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    vp = np.sum((y_true == 1) & (y_pred == 1))
    fn = np.sum((y_true == 1) & (y_pred == 0))
    return vp / (vp + fn) if (vp + fn) > 0 else 0.0


def f1_score_scratch(y_true, y_pred):
    """Calcula o F1-Score."""
    p = precision_score_scratch(y_true, y_pred)
    r = recall_score_scratch(y_true, y_pred)
    return 2 * p * r / (p + r) if (p + r) > 0 else 0.0


def balanced_accuracy_score_scratch(y_true, y_pred):
    """Calcula a acurácia balanceada."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    classes = np.unique(y_true)
    recalls = []
    for c in classes:
        mask = y_true == c
        recalls.append(np.mean(y_pred[mask] == c))
    return np.mean(recalls)


def confusion_matrix_scratch(y_true, y_pred):
    """Calcula a matriz de confusão (2x2 para binário)."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    cm = np.zeros((2, 2), dtype=int)
    for t, p in zip(y_true, y_pred):
        cm[int(t), int(p)] += 1
    return cm


def roc_auc_score_scratch(y_true, y_proba):
    """
    Calcula o AUC-ROC usando o método trapezoidal.

    Implementação from scratch:
    1. Ordena por score decrescente
    2. Calcula TPR e FPR em cada threshold
    3. Integra com regra dos trapézios
    """
    y_true = np.asarray(y_true, dtype=int)
    y_proba = np.asarray(y_proba, dtype=float)

    n_pos = np.sum(y_true == 1)
    n_neg = np.sum(y_true == 0)

    if n_pos == 0 or n_neg == 0:
        return 0.5

    # Ordena por score decrescente
    sorted_indices = np.argsort(-y_proba)
    sorted_true = y_true[sorted_indices]

    # Calcula TPR e FPR para cada threshold (scores como threshold)
    tpr_list = [0.0]
    fpr_list = [0.0]

    tp = 0
    fp = 0
    for i, label in enumerate(sorted_true):
        if label == 1:
            tp += 1
        else:
            fp += 1
        tpr_list.append(tp / n_pos)
        fpr_list.append(fp / n_neg)

    tpr_list = np.array(tpr_list)
    fpr_list = np.array(fpr_list)

    # AUC via regra dos trapézios (np.trapezoid é o nome atual; fallback para np.trapz)
    _trapz = getattr(np, 'trapezoid', getattr(np, 'trapz', None))
    auc = _trapz(tpr_list, fpr_list)
    return abs(auc)  # garante positivo


def classification_report_scratch(y_true, y_pred, target_names=None):
    """Gera um relatório de classificação."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    classes = np.unique(np.concatenate([y_true, y_pred]))
    if target_names is None:
        target_names = [str(c) for c in classes]

    lines = []
    lines.append(f"{'':>20} {'precision':>10} {'recall':>10} {'f1-score':>10} {'support':>10}")
    lines.append("")

    per_class_prec = []
    per_class_rec = []
    per_class_f1 = []
    per_class_support = []

    for i, c in enumerate(classes):
        mask_true = y_true == c
        mask_pred = y_pred == c
        tp = np.sum(mask_true & mask_pred)
        fp = np.sum(~mask_true & mask_pred)
        fn = np.sum(mask_true & ~mask_pred)
        support = np.sum(mask_true)

        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0

        per_class_prec.append(prec)
        per_class_rec.append(rec)
        per_class_f1.append(f1)
        per_class_support.append(support)

        name = target_names[i] if i < len(target_names) else str(c)
        lines.append(f"{name:>20} {prec:>10.4f} {rec:>10.4f} {f1:>10.4f} {support:>10}")

    # Médias
    lines.append("")
    lines.append(f"{'accuracy':>20} {'':>10} {'':>10} "
                 f"{accuracy_score_scratch(y_true, y_pred):>10.4f} {len(y_true):>10}")

    # Macro avg (média simples das métricas por classe)
    macro_prec = np.mean(per_class_prec)
    macro_rec = np.mean(per_class_rec)
    macro_f1 = np.mean(per_class_f1)
    lines.append(f"{'macro avg':>20} {macro_prec:>10.4f} {macro_rec:>10.4f} "
                 f"{macro_f1:>10.4f} {len(y_true):>10}")

    # Weighted avg (média ponderada pelo suporte)
    total_support = sum(per_class_support)
    w_prec = sum(p * s for p, s in zip(per_class_prec, per_class_support)) / total_support if total_support > 0 else 0
    w_rec = sum(r * s for r, s in zip(per_class_rec, per_class_support)) / total_support if total_support > 0 else 0
    w_f1 = sum(f * s for f, s in zip(per_class_f1, per_class_support)) / total_support if total_support > 0 else 0
    lines.append(f"{'weighted avg':>20} {w_prec:>10.4f} {w_rec:>10.4f} "
                 f"{w_f1:>10.4f} {len(y_true):>10}")

    return "\n".join(lines)


# =============================================================================
# 4. IMPLEMENTAÇÃO DO ZERO: TRAIN/TEST SPLIT ESTRATIFICADO
# =============================================================================

def train_test_split_scratch(X, y, test_size=0.25, random_state=None, stratify=None):
    """
    Divide dados em treino e teste, preservando proporção de classes.

    Parameters
    ----------
    X : ndarray
    y : ndarray
    test_size : float
        Fração do dataset para teste.
    random_state : int ou None
    stratify : ndarray ou None
        Se fornecido, usa stratificação.

    Returns
    -------
    X_train, X_test, y_train, y_test
    """
    rng = np.random.RandomState(random_state)
    n = len(y)

    if stratify is not None:
        stratify = np.asarray(stratify)
        classes = np.unique(stratify)
        train_indices = []
        test_indices = []

        for c in classes:
            idx = np.where(stratify == c)[0]
            rng.shuffle(idx)
            n_test = max(1, int(len(idx) * test_size))
            test_indices.extend(idx[:n_test])
            train_indices.extend(idx[n_test:])

        train_indices = np.array(train_indices)
        test_indices = np.array(test_indices)
        rng.shuffle(train_indices)
        rng.shuffle(test_indices)
    else:
        indices = np.arange(n)
        rng.shuffle(indices)
        n_test = int(n * test_size)
        test_indices = indices[:n_test]
        train_indices = indices[n_test:]

    return X[train_indices], X[test_indices], y[train_indices], y[test_indices]


# =============================================================================
# 5. IMPLEMENTAÇÃO DO ZERO: STRATIFIED K-FOLD CROSS-VALIDATION
# =============================================================================

def stratified_kfold_split(y, n_splits=5, shuffle=True, random_state=None):
    """
    Gera índices de Stratified K-Fold.

    Yields
    ------
    train_idx, val_idx para cada fold
    """
    rng = np.random.RandomState(random_state)
    y = np.asarray(y)
    classes = np.unique(y)

    # Agrupa índices por classe
    class_indices = {}
    for c in classes:
        idx = np.where(y == c)[0]
        if shuffle:
            rng.shuffle(idx)
        class_indices[c] = idx

    # Divide cada classe em n_splits partes
    folds = [[] for _ in range(n_splits)]
    for c in classes:
        idx = class_indices[c]
        fold_sizes = np.full(n_splits, len(idx) // n_splits)
        fold_sizes[:len(idx) % n_splits] += 1
        current = 0
        for k in range(n_splits):
            folds[k].extend(idx[current:current + fold_sizes[k]])
            current += fold_sizes[k]

    for k in range(n_splits):
        val_idx = np.array(folds[k])
        train_idx = np.concatenate([folds[j] for j in range(n_splits) if j != k])
        rng.shuffle(train_idx)
        rng.shuffle(val_idx)
        yield train_idx, val_idx


def cross_val_score_scratch(model_class, X, y, cv=5, scoring='f1',
                            model_params=None, random_state=None):
    """
    Cross-validation genérica do zero.

    Parameters
    ----------
    model_class : classe do modelo
    X, y : arrays
    cv : int
    scoring : str ('f1', 'accuracy', 'precision', 'recall')
    model_params : dict
    """
    if model_params is None:
        model_params = {}

    scoring_fn = {
        'f1': f1_score_scratch,
        'accuracy': accuracy_score_scratch,
        'precision': precision_score_scratch,
        'recall': recall_score_scratch,
    }.get(scoring, f1_score_scratch)

    scores = []
    for train_idx, val_idx in stratified_kfold_split(
        y, n_splits=cv, shuffle=True, random_state=random_state
    ):
        X_train_fold = X[train_idx]
        y_train_fold = y[train_idx]
        X_val_fold = X[val_idx]
        y_val_fold = y[val_idx]

        model = model_class(**model_params)
        model.fit(X_train_fold, y_train_fold)
        y_pred = model.predict(X_val_fold)
        scores.append(scoring_fn(y_val_fold, y_pred))

    return np.array(scores)


# =============================================================================
# 6. NORMALIZAÇÃO (MinMaxScaler - permitido conforme solicitado)
# =============================================================================

class MinMaxScalerFromScratch:
    """
    Min-Max Scaler: normaliza features para [0, 1].

    Implementação from scratch usando numpy.
    x_scaled = (x - x_min) / (x_max - x_min)
    """

    def __init__(self):
        self.min_ = None
        self.max_ = None
        self.scale_ = None
        self.n_features = 0

    def fit(self, X):
        """Aprende min e max de cada feature."""
        X = np.asarray(X, dtype=np.float64)
        self.min_ = X.min(axis=0)
        self.max_ = X.max(axis=0)
        self.scale_ = self.max_ - self.min_
        # Evita divisão por zero (feature constante)
        self.scale_[self.scale_ == 0] = 1.0
        self.n_features = X.shape[1]
        return self

    def transform(self, X):
        """Transforma X usando min e max aprendidos."""
        X = np.asarray(X, dtype=np.float64)
        return (X - self.min_) / self.scale_

    def fit_transform(self, X):
        """Fit + transform."""
        return self.fit(X).transform(X)


# =============================================================================
# 7. IMAGENS: EXTRAÇÃO DE FEATURES
# =============================================================================

CSV_PATH = 'datatran2026.csv'
THRESHOLD = 0.37

IMAGENS_DIR = os.path.join('..', 'mapas')
COLUNA_CAMINHO_IMAGEM = 'caminho_imagem'
COLUNA_ID_IMAGEM = 'id'
PREFIXO_IMAGEM = 'mapa_'
EXTENSAO_IMAGEM = '.png'

IMG_RESIZE_SIZE = (256, 256)
COLUNAS_IMAGEM = ['prop_via_principal', 'prop_vegetacao']

FAIXA_VIA_LARANJA = ((0, 70, 180), (15, 180, 255))
FAIXA_VIA_ROSA = ((165, 40, 180), (180, 180, 255))
FAIXA_VEGETACAO = ((35, 40, 40), (85, 255, 255))


def extrair_features_imagem(caminho_imagem):
    """Extrai proporções de via principal e vegetação de uma imagem de mapa."""
    if not caminho_imagem or not os.path.exists(caminho_imagem):
        return np.zeros(len(COLUNAS_IMAGEM), dtype=np.float32)

    img_bgr = cv2.imread(caminho_imagem)
    if img_bgr is None:
        return np.zeros(len(COLUNAS_IMAGEM), dtype=np.float32)

    img_bgr = cv2.resize(img_bgr, IMG_RESIZE_SIZE)
    img_hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
    total_pixels = IMG_RESIZE_SIZE[0] * IMG_RESIZE_SIZE[1]

    mascara_laranja = cv2.inRange(
        img_hsv, np.array(FAIXA_VIA_LARANJA[0]), np.array(FAIXA_VIA_LARANJA[1])
    )
    mascara_rosa = cv2.inRange(
        img_hsv, np.array(FAIXA_VIA_ROSA[0]), np.array(FAIXA_VIA_ROSA[1])
    )
    mascara_via_principal = cv2.bitwise_or(mascara_laranja, mascara_rosa)
    prop_via_principal = float(np.count_nonzero(mascara_via_principal)) / total_pixels

    mascara_vegetacao = cv2.inRange(
        img_hsv, np.array(FAIXA_VEGETACAO[0]), np.array(FAIXA_VEGETACAO[1])
    )
    prop_vegetacao = float(np.count_nonzero(mascara_vegetacao)) / total_pixels

    return np.array([prop_via_principal, prop_vegetacao], dtype=np.float32)


def obter_caminho_imagem(linha):
    """Prioriza a coluna de caminho; sem ela, monta ../mapas/mapa_<id>.png."""
    caminho = linha.get(COLUNA_CAMINHO_IMAGEM)
    if pd.notna(caminho) and str(caminho).strip():
        return str(caminho).strip()

    identificador = linha.get(COLUNA_ID_IMAGEM)
    if pd.isna(identificador):
        return ''
    return os.path.join(IMAGENS_DIR, f'{PREFIXO_IMAGEM}{identificador}{EXTENSAO_IMAGEM}')


# =============================================================================
# 8. PIPELINE PRINCIPAL
# =============================================================================

if __name__ == '__main__':

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    np.random.seed(42)

    print("=" * 70)
    print("PIPELINE BINÁRIO - PREVISÃO DE ACIDENTES COM VÍTIMAS (PRF)")
    print("IMPLEMENTAÇÃO DO ZERO (HARD-CODED)")
    print(f"Threshold definido: {THRESHOLD}")
    print("=" * 70)

    # ---------------------------------------------------------------------------
    # 8.1 Carregamento dos Dados
    # ---------------------------------------------------------------------------
    print("\n[1/6] Carregando dados...")

    df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig', low_memory=False)
    print(f"✓ Dataset carregado: {df.shape[0]:,} linhas x {df.shape[1]} colunas")

    # ---------------------------------------------------------------------------
    # 8.2 Limpeza e Feature Engineering
    # ---------------------------------------------------------------------------
    print("\n[2/6] Limpeza e Feature Engineering...")

    df_model = df.copy()

    # Limpar target
    df_model['classificacao_acidente'] = df_model['classificacao_acidente'].str.strip()
    df_model = df_model[df_model['classificacao_acidente'].notna()]
    df_model = df_model[df_model['classificacao_acidente'] != 'NA']
    df_model = df_model[df_model['classificacao_acidente'] != '']


    def normalizar_classificacao(valor):
        valor = str(valor).strip().lower()
        if 'fatal' in valor or 'mort' in valor:
            return 'Com Vítimas Fatais'
        elif 'ferid' in valor or 'les' in valor:
            return 'Com Vítimas Feridas'
        elif 'sem' in valor or 'nenhum' in valor or 'iles' in valor:
            return 'Sem Vítimas'
        else:
            return 'Outro'


    df_model['classificacao_acidente_norm'] = df_model['classificacao_acidente'].apply(
        normalizar_classificacao
    )

    # Feature Engineering
    df_model['hora'] = pd.to_datetime(
        df_model['horario'], format='%H:%M:%S', errors='coerce'
    ).dt.hour


    def faixa_horaria(hora):
        if pd.isna(hora):
            return 'Desconhecido'
        elif 0 <= hora < 6:
            return 'Madrugada'
        elif 6 <= hora < 12:
            return 'Manhã'
        elif 12 <= hora < 18:
            return 'Tarde'
        else:
            return 'Noite'


    df_model['faixa_horaria'] = df_model['hora'].apply(faixa_horaria)

    df_model['fim_de_semana'] = df_model['dia_semana'].apply(
        lambda x: 'Sim' if str(x).lower() in ['sábado', 'sabado', 'domingo'] else 'Não'
    )

    regioes = {
        'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte',
        'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste',
        'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste',
        'SE': 'Nordeste',
        'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste',
        'MS': 'Centro-Oeste',
        'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }
    df_model['regiao'] = df_model['uf'].map(regioes).fillna('Desconhecida')


    def agrupar_causa(causa):
        causa = str(causa).lower()
        if any(p in causa for p in ['velocidade', 'ultrapass', 'distância', 'distancia']):
            return 'Velocidade/Ultrapassagem'
        elif any(p in causa for p in ['dormir', 'sono', 'fadiga']):
            return 'Fadiga/Sono'
        elif any(p in causa for p in ['álcool', 'alcool', 'drog', 'embriag']):
            return 'Álcool/Drogas'
        elif any(p in causa for p in ['chuva', 'molha', 'escorreg', 'pista']):
            return 'Condição da Pista'
        elif any(p in causa for p in ['defeito', 'falha', 'pneu', 'mecânica', 'mecanica']):
            return 'Defeito Mecânico'
        elif any(p in causa for p in ['animal', 'pedestre', 'objeto']):
            return 'Obstáculo na Via'
        elif any(p in causa for p in ['não guardar', 'nao guardar', 'preferência',
                                       'preferencia', 'sinal']):
            return 'Desrespeito às Normas'
        elif any(p in causa for p in ['inexperiência', 'inexperiencia', 'habilidade']):
            return 'Inexperiência'
        elif any(p in causa for p in ['celular', 'distração', 'distraçao',
                                       'atenção', 'atencao']):
            return 'Distração'
        else:
            return 'Outras'


    df_model['causa_agrupada'] = df_model['causa_acidente'].apply(agrupar_causa)


    def simplificar_tracado(tracado):
        tracado = str(tracado).lower()
        if 'reta' in tracado and 'curva' not in tracado:
            return 'Reta'
        elif 'curva' in tracado and 'reta' not in tracado:
            return 'Curva'
        elif 'reta' in tracado and 'curva' in tracado:
            return 'Misto'
        elif 'interse' in tracado or 'cruz' in tracado:
            return 'Interseção'
        elif 'rot' in tracado:
            return 'Rotatória'
        elif 'ponte' in tracado or 'viadu' in tracado:
            return 'Ponte/Viaduto'
        else:
            return 'Outro'


    df_model['tracado_simplificado'] = df_model['tracado_via'].apply(simplificar_tracado)

    df_model['horario_perigoso'] = df_model['hora'].apply(
        lambda x: 'Sim' if pd.notna(x) and (x < 6 or x > 22) else 'Não'
    )

    print(f"✓ {df_model.shape[0]:,} registros válidos")

    # ---------------------------------------------------------------------------
    # 8.3 Target Binário e Features
    # ---------------------------------------------------------------------------
    print("\n[3/6] Preparando target binário e features...")

    y_bin = df_model['classificacao_acidente_norm'].apply(
        lambda x: 0 if x == 'Sem Vítimas' else 1
    ).values

    print(f"\nDistribuição do target binário:")
    unique, counts = np.unique(y_bin, return_counts=True)
    for u, c in zip(unique, counts):
        print(f"  {u}: {c:,} ({c / len(y_bin) * 100:.2f}%)")

    features_categoricas = [
        'dia_semana', 'uf', 'causa_agrupada', 'tipo_acidente',
        'fase_dia', 'sentido_via', 'condicao_metereologica',
        'tipo_pista', 'uso_solo', 'faixa_horaria', 'fim_de_semana',
        'regiao', 'tracado_simplificado', 'horario_perigoso'
    ]

    X_categoricas = df_model[features_categoricas].copy().fillna('Desconhecido')
    X_categoricas_encoded = pd.get_dummies(X_categoricas, drop_first=False)

    # Extrair features de imagem
    caminhos_imagem = df_model.apply(obter_caminho_imagem, axis=1)
    qtd_imagens_encontradas = caminhos_imagem.apply(os.path.exists).sum()
    features_imagem = np.vstack(caminhos_imagem.map(extrair_features_imagem).to_numpy())
    X_imagem = pd.DataFrame(features_imagem, columns=COLUNAS_IMAGEM, index=df_model.index)

    X_encoded = pd.concat([X_categoricas_encoded, X_imagem], axis=1).astype(np.float64)

    print(f"\nFeatures após encoding: {X_encoded.shape[1]} colunas")
    print(f"Features de imagem: {COLUNAS_IMAGEM} | imagens encontradas: "
          f"{qtd_imagens_encontradas:,}/{len(df_model):,}")

    # Converter para numpy
    X_np = X_encoded.values.astype(np.float64)
    feature_names = list(X_encoded.columns)

    # ---------------------------------------------------------------------------
    # 8.4 Normalização (MinMaxScaler do zero)
    # ---------------------------------------------------------------------------
    print("\nNormalizando features com MinMaxScaler (implementação do zero)...")

    scaler = MinMaxScalerFromScratch()
    X_np = scaler.fit_transform(X_np)
    print(f"✓ Features normalizadas para [0, 1]")

    # ---------------------------------------------------------------------------
    # 8.5 Train/Test Split e SMOTE
    # ---------------------------------------------------------------------------
    print("\n[4/6] Dividindo dados e aplicando SMOTE...")

    X_train, X_test, y_train, y_test = train_test_split_scratch(
        X_np, y_bin, test_size=0.25, random_state=42, stratify=y_bin
    )

    print(f"Treino: {X_train.shape[0]:,} | Teste: {X_test.shape[0]:,}")

    smote = SMOTEFromScratch(k_neighbors=5, random_state=42)
    X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

    unique_train_before, counts_train_before = np.unique(y_train, return_counts=True)
    unique_train_after, counts_train_after = np.unique(y_train_smote, return_counts=True)

    print(f"\nAntes do SMOTE: {dict(zip(unique_train_before.tolist(), counts_train_before.tolist()))}")
    print(f"Depois do SMOTE: {dict(zip(unique_train_after.tolist(), counts_train_after.tolist()))}")

    # ---------------------------------------------------------------------------
    # 8.6 Treinamento do Random Forest (do zero)
    # ---------------------------------------------------------------------------
    print("\n[5/6] Treinando Random Forest (implementação do zero)...")

    start = time.time()

    rf = RandomForestClassifierFromScratch(
        n_estimators=300,
        max_depth=30,
        min_samples_split=5,
        min_samples_leaf=2,
        max_features='sqrt',
        random_state=42,
        n_jobs=-1
    )

    rf.fit(X_train_smote, y_train_smote)

    tempo_treino = time.time() - start
    print(f"✓ Treinamento concluído em {tempo_treino:.2f}s")

    # ---------------------------------------------------------------------------
    # 8.7 Avaliação com Threshold Ajustável
    # ---------------------------------------------------------------------------
    print("\n[6/6] Avaliando modelo...")

    y_proba = rf.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= THRESHOLD).astype(int)

    print("\n" + "=" * 70)
    print(f"MÉTRICAS DE PERFORMANCE (Threshold = {THRESHOLD})")
    print("=" * 70)
    print(f"Accuracy:     {accuracy_score_scratch(y_test, y_pred):.4f}")
    print(f"Balanced Acc: {balanced_accuracy_score_scratch(y_test, y_pred):.4f}")
    print(f"Precision:    {precision_score_scratch(y_test, y_pred):.4f}")
    print(f"Recall:       {recall_score_scratch(y_test, y_pred):.4f}")
    print(f"F1-Score:     {f1_score_scratch(y_test, y_pred):.4f}")
    print(f"AUC-ROC:      {roc_auc_score_scratch(y_test, y_proba):.4f}")

    # Cross-validation
    print("\n--- Cross-Validation (5 folds) ---")

    def _make_model():
        """Factory para cross_val_score."""
        return RandomForestClassifierFromScratch(
            n_estimators=300, max_depth=30,
            min_samples_split=5, min_samples_leaf=2,
            max_features='sqrt', random_state=42
        )

    cv_scores = []
    for train_idx, val_idx in stratified_kfold_split(
        y_train_smote, n_splits=5, shuffle=True, random_state=42
    ):
        model_cv = _make_model()
        model_cv.fit(X_train_smote[train_idx], y_train_smote[train_idx])
        y_pred_cv = model_cv.predict(X_train_smote[val_idx])
        cv_scores.append(f1_score_scratch(y_train_smote[val_idx], y_pred_cv))

    cv_scores = np.array(cv_scores)
    print(f"F1-Score médio (CV): {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")

    # Classification Report
    print("\n--- Classification Report ---")
    print(classification_report_scratch(y_test, y_pred,
                                        target_names=['Sem Vítimas', 'Com Vítimas']))

    # Matriz de Confusão
    print("\n--- Matriz de Confusão ---")
    cm = confusion_matrix_scratch(y_test, y_pred)
    print(pd.DataFrame(cm,
                       index=['Real: Sem Vítimas', 'Real: Com Vítimas'],
                       columns=['Pred: Sem Vítimas', 'Pred: Com Vítimas']))

    # Análise de impacto
    vp = int(np.sum((y_test == 1) & (y_pred == 1)))
    vn = int(np.sum((y_test == 0) & (y_pred == 0)))
    fp = int(np.sum((y_test == 0) & (y_pred == 1)))
    fn = int(np.sum((y_test == 1) & (y_pred == 0)))

    print("\n" + "=" * 70)
    print(f"ANÁLISE DE IMPACTO PRÁTICO (Threshold = {THRESHOLD})")
    print("=" * 70)
    print(f"Total de acidentes no teste: {len(y_test):,}")
    print(f"\n✓ Vítimas CORRETAMENTE atendidas:       {vp:,} ({vp / (vp + fn) * 100:.1f}%)")
    print(f"✓ Não-despachos corretos:              {vn:,}")
    print(f"⚠ Despachos DESNECESSÁRIOS:            {fp:,} ({fp / (fp + vn) * 100:.1f}%)")
    print(f"✗ Vítimas NÃO atendidas:               {fn:,} ({fn / (vp + fn) * 100:.1f}%)")

    # ---------------------------------------------------------------------------
    # 8.8 Comparação de Thresholds
    # ---------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("COMPARAÇÃO DE THRESHOLDS")
    print("=" * 70)

    thresholds_teste = [0.3, 0.37, 0.4, 0.5, 0.6, 0.7]
    print(f"{'Threshold':>10} | {'Accuracy':>8} | {'Precision':>9} | "
          f"{'Recall':>6} | {'F1':>6} | {'FN':>6} | {'FP':>6}")
    print("-" * 70)

    for t in thresholds_teste:
        y_p = (y_proba >= t).astype(int)
        acc = accuracy_score_scratch(y_test, y_p)
        prec = precision_score_scratch(y_test, y_p)
        rec = recall_score_scratch(y_test, y_p)
        f1 = f1_score_scratch(y_test, y_p)
        fn_t = int(np.sum((y_test == 1) & (y_p == 0)))
        fp_t = int(np.sum((y_test == 0) & (y_p == 1)))
        marker = " <--" if t == THRESHOLD else ""
        print(f"{t:>10.2f} | {acc:>8.4f} | {prec:>9.4f} | {rec:>6.4f} | "
              f"{f1:>6.4f} | {fn_t:>6} | {fp_t:>6}{marker}")

    # ---------------------------------------------------------------------------
    # 8.9 Visualizações
    # ---------------------------------------------------------------------------
    print("\nGerando visualizações...")

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f'Resultados - Modelo Binário (Implementação do Zero)\n'
                 f'(Threshold = {THRESHOLD})',
                 fontsize=14, fontweight='bold')

    # 1. Curva ROC
    fpr_list = [0.0]
    tpr_list = [0.0]
    n_pos = np.sum(y_test == 1)
    n_neg = np.sum(y_test == 0)
    sorted_idx = np.argsort(-y_proba)
    sorted_labels = y_test[sorted_idx]
    tp = 0
    fp = 0
    for label in sorted_labels:
        if label == 1:
            tp += 1
            tpr_list.append(tp / n_pos)
            fpr_list.append(fp / n_neg)
        else:
            fp += 1
            tpr_list.append(tp / n_pos)
            fpr_list.append(fp / n_neg)

    auc_val = roc_auc_score_scratch(y_test, y_proba)
    axes[0, 0].plot(fpr_list, tpr_list, color='darkorange', lw=2,
                    label=f'ROC (AUC = {auc_val:.3f})')
    axes[0, 0].plot([0, 1], [0, 1], color='navy', lw=1, linestyle='--')
    axes[0, 0].fill_between(fpr_list, tpr_list, alpha=0.2, color='darkorange')
    axes[0, 0].set_xlabel('False Positive Rate')
    axes[0, 0].set_ylabel('True Positive Rate')
    axes[0, 0].set_title('Curva ROC')
    axes[0, 0].legend(loc='lower right')
    axes[0, 0].grid(alpha=0.3)

    # 2. Matriz de Confusão
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[0, 1],
                xticklabels=['Sem Vítimas', 'Com Vítimas'],
                yticklabels=['Sem Vítimas', 'Com Vítimas'])
    axes[0, 1].set_title(f'Matriz de Confusão (threshold={THRESHOLD})')
    axes[0, 1].set_xlabel('Predito')
    axes[0, 1].set_ylabel('Real')

    # 3. Métricas em barras
    metricas_nomes = ['Accuracy', 'Precision', 'Recall', 'F1-Score']
    metricas_valores = [
        accuracy_score_scratch(y_test, y_pred),
        precision_score_scratch(y_test, y_pred),
        recall_score_scratch(y_test, y_pred),
        f1_score_scratch(y_test, y_pred),
    ]
    bars = axes[1, 0].bar(metricas_nomes, metricas_valores,
                           color=['#2ecc71', '#3498db', '#e74c3c', '#9b59b6'])
    axes[1, 0].set_ylim(0, 1)
    axes[1, 0].set_ylabel('Score')
    axes[1, 0].set_title('Métricas de Performance')
    for bar, val in zip(bars, metricas_valores):
        axes[1, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                        f'{val:.3f}', ha='center', va='bottom', fontweight='bold')

    # 4. Feature Importance
    importancias = pd.DataFrame({
        'feature': feature_names,
        'importance': rf.feature_importances_
    }).sort_values('importance', ascending=False).head(15)

    axes[1, 1].barh(importancias['feature'][::-1], importancias['importance'][::-1],
                    color='steelblue')
    axes[1, 1].set_xlabel('Importância')
    axes[1, 1].set_title('Top 15 Features Mais Importantes')
    axes[1, 1].grid(axis='x', alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'resultados_do_zero_threshold_{THRESHOLD}.png', dpi=150,
                bbox_inches='tight')
    plt.show()
    print(f"✓ resultados_do_zero_threshold_{THRESHOLD}.png salvo")

    # ---------------------------------------------------------------------------
    # 8.10 Salvar modelo
    # ---------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("PIPELINE BINÁRIO CONCLUÍDO! (Implementação do Zero)")
    print(f"Threshold = {THRESHOLD}")
    print("=" * 70)
