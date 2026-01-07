import express from 'express';
import cors from 'cors';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;
const pool = new Pool({
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
});

const app = express();
app.use(cors({
  origin: ['http://localhost:3000'],
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  credentials: true
}));

pool.connect((err, client, release) => {
  if (err) {
    return console.error('Error acquiring client', err.stack);
  }
  console.log('Connected to PostgreSQL database');
  release();
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'BI Dashboard running' });
});

app.get('/api/stats', async (req, res) => {
  try {
    const userCount = await pool.query('SELECT COUNT(*) FROM dim_user');
    const movieCount = await pool.query('SELECT COUNT(*) FROM dim_movie');
    const viewsCount = await pool.query('SELECT COUNT(*) FROM fact_views');

    res.json({
      users: parseInt(userCount.rows[0].count, 10),
      movies: parseInt(movieCount.rows[0].count, 10),
      views: parseInt(viewsCount.rows[0].count, 10),
    });
  } catch (err) {
    console.error('Error fetching stats:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});


app.get('/api/charts/genre-distribution', async (req, res) => {
  try {
    const query = `
      SELECT m.genre, SUM(f.total_views) as views
      FROM fact_views f
      JOIN dim_movie m ON f.movie_id = m.movie_id
      GROUP BY m.genre
      ORDER BY views DESC
      LIMIT 10;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching genre stats:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/charts/views-over-time', async (req, res) => {
  try {
    const query = `
      SELECT d.date, SUM(f.total_views) as views
      FROM fact_views f
      JOIN dim_date d ON f.date_id = d.date_id
      GROUP BY d.date
      ORDER BY d.date ASC
      LIMIT 30; 
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching timeline stats:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/charts/user-distribution', async (req, res) => {
  try {
    const query = `
      SELECT age_group, COUNT(user_id) as count
      FROM dim_user
      GROUP BY age_group
      ORDER BY count DESC;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching user stats:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/charts/top-movies', async (req, res) => {
  try {
    const query = `
      SELECT m.title, SUM(f.total_views) as views
      FROM fact_views f
      JOIN dim_movie m ON f.movie_id = m.movie_id
      GROUP BY m.title, m.movie_id
      ORDER BY views DESC
      LIMIT 10;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching top movies:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/charts/users-by-country', async (req, res) => {
  try {
    const query = `
      SELECT country, COUNT(user_id) as user_count
      FROM dim_user
      GROUP BY country
      ORDER BY user_count DESC
      LIMIT 15;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching users by country:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.get('/api/charts/revenue-timeline', async (req, res) => {
  try {
    const query = `
      SELECT d.date, SUM(f.total_views * 0.05) as total_revenue
      FROM fact_views f
      JOIN dim_date d ON f.date_id = d.date_id
      GROUP BY d.date
      ORDER BY d.date ASC
      LIMIT 30;
    `;
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error('Error fetching revenue timeline:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

const PORT = process.env.PORT ; 
app.listen(PORT, () => {
  console.log(`Backend running on port ${PORT}`);
});
