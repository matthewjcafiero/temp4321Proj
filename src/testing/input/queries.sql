SELECT * FROM Sailors WHERE Sailors.A < 1000;
SELECT * FROM Boats WHERE Boats.E > 8000;
SELECT * FROM Sailors WHERE Sailors.C > 8000;
SELECT * FROM Sailors, Boats WHERE Sailors.A = Boats.E AND Sailors.A < 800;
SELECT * FROM Sailors S WHERE S.A >= 5000 ORDER BY S.C;
SELECT * FROM Sailors S WHERE S.A = 2230
SELECT S.A FROM Sailors S WHERE S.A = 2230
SELECT * FROM Sailors S WHERE S.B = 0
SELECT * FROM Sailors S WHERE S.A != 2230
SELECT * FROM Sailors, Boats WHERE Sailors.A = 2230 and Boats.E < 100;
SELECT * FROM Sailors, Boats WHERE Sailors.A = 2230 and Boats.D <= 50;
SELECT * FROM Sailors, Boats WHERE Sailors.A = 2230 and Boats.D < 50;
SELECT * FROM Sailors, Boats WHERE Boats.D = 50
SELECT * FROM Sailors, Boats WHERE Boats.E = 41
SELECT * FROM Sailors, Boats WHERE Sailors.A = Boats.E AND Sailors.B = Boats.D
SELECT * FROM Sailors S, Sailors S1 WHERE S.A = 2230 AND S1.A = S.A
SELECT * FROM Sailors S, Sailors S1, Sailors S2 WHERE S.A = 2230 AND S1.A = 2230 AND S2.A = 2230
SELECT * FROM Sailors S, Sailors S1, Boats B WHERE S.A = 2230 AND S1.A = 2230
SELECT * FROM Sailors S, Sailors S1, Boats B WHERE S.A = 2230 AND S1.A = 2230 AND B.E < 100
SELECT * FROM Sailors S, Boats B, Sailors S1 WHERE S.A = 2230 AND S1.A = 2230 AND B.E < 100
SELECT * FROM Sailors S, Boats B, Sailors S1 WHERE S.A = 2230 AND S1.A = 2230 AND B.E < S.A
SELECT * FROM Boats WHERE Boats.E < 2230
SELECT * FROM Sailors S, Boats B WHERE S.A = 2230 AND B.E < S.A
SELECT * FROM Sailors S, Sailors S1, Boats B WHERE S.A = 2230 AND S1.A = 2230 AND B.E < S.A
