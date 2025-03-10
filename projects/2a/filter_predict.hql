INSERT INTO TABLE KiruhaLapin_hw2_pred
SELECT id, model.predict(col2) AS prediction
FROM hw2_test
WHERE 20 < if1 AND if1 < 40;

