/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import Typography from '@mui/material/Typography';
import {
  Box,
  Button,
  Checkbox,
  // TODO: update when MUI is updated to version 6
  Unstable_Grid2 as Grid,
  Link,
  Paper,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow
} from "@mui/material";

export const Dashboard = () => {
  const createData = (
    name: string,
    calories: number,
    fat: number,
    carbs: number,
    protein: number,
  ) => {
    return { name, calories, fat, carbs, protein };
  }

  const rows = [
    createData('Frozen yoghurt', 159, 6.0, 24, 4.0),
    createData('Ice cream sandwich', 237, 9.0, 37, 4.3),
    createData('Eclair', 262, 16.0, 24, 6.0),
  ];

  return (
    <>
      <Box sx={{ pb: 2 }}>
        <Typography variant="h4">
          Dashboard
        </Typography>
      </Box>
      <Grid container>
        <Grid sx={{ py: 1 }} xs={12} container>
          <Grid xs={3}>Buttons</Grid>
          <Stack direction="row" spacing={2}>
            <Button variant="contained">Contained</Button>
            <Button variant="outlined">Outlined</Button>
            <Button variant="contained" disabled>Disabled</Button>
          </Stack>
        </Grid>

        <Grid sx={{ py: 1 }} xs={12} container>
          <Grid xs={3}>Secondary Button</Grid>
          <Stack direction="row" spacing={2}>
            <Button color="secondary" variant="contained">Contained</Button>
            <Button color="secondary" variant="outlined">Outlined</Button>
          </Stack>
        </Grid>

        <Grid sx={{ py: 1 }} xs={12} container>
          <Grid xs={3}>Colored Button</Grid>
          <Stack direction="row" spacing={2}>
            <Button variant="contained" color="success">Success</Button>
            <Button variant="contained" color="error">Error</Button>
          </Stack>
        </Grid>

        <Grid sx={{ py: 1 }} xs={12} container>
          <Grid xs={3}>Link</Grid>
          <Link href="https://trino.io">Trino Website</Link>
        </Grid>

        <Grid sx={{ py: 1 }} xs={12} container>
          <Grid xs={3}>Switch</Grid>
          <Switch />
        </Grid>

        <Grid sx={{ py: 1 }} xs={12} container>
          <Grid xs={3}>Checkbox</Grid>
          <Checkbox defaultChecked />
        </Grid>
      </Grid>

      <TableContainer component={Paper}>
        <Table sx={{ minWidth: 650 }} aria-label="simple table">
          <TableHead>
            <TableRow>
              <TableCell>Dessert (100g serving)</TableCell>
              <TableCell align="right">Calories</TableCell>
              <TableCell align="right">Fat&nbsp;(g)</TableCell>
              <TableCell align="right">Carbs&nbsp;(g)</TableCell>
              <TableCell align="right">Protein&nbsp;(g)</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map((row) => (
              <TableRow
                key={row.name}
                sx={{ '&:last-child td, &:last-child th': { border: 0 } }}
              >
                <TableCell component="th" scope="row">
                  {row.name}
                </TableCell>
                <TableCell align="right">{row.calories}</TableCell>
                <TableCell align="right">{row.fat}</TableCell>
                <TableCell align="right">{row.carbs}</TableCell>
                <TableCell align="right">{row.protein}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
