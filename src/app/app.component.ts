import { Component, OnInit } from '@angular/core';
import { Form } from '@angular/forms'
import { HttpClient } from '@angular/common/http';
import { ChartType } from 'chart.js';
import { MultiDataSet, Label } from 'ng2-charts';

//import { AngularFireDatabase } from '@angular/fire/database';


@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  title = 'app'
  test: any;
  skillsNeeded: any;
  //relJobs: any;
  relJobs: any;
  searchQuery = '';
  saveQuery = '';
  isSkill = false;
  isJob = false;
  isNull = false;
  doughnutChartLabels = ['1','2','3'];
  doughnutChartData = [120,150,90];
  public doughnutChartType: string = 'doughnut';

//  constructor(private db: AngularFireDatabase){}
  root_url = 'https://work-search-3dd70.firebaseio.com/';

  constructor(private http: HttpClient) {}

  getKeySkills(){
    this.isSkill = true;
    this.isJob = false;
    this.isNull = false;
    this.saveQuery = this.searchQuery.toLowerCase();
    console.log(this.searchQuery)
    this.http.get(this.root_url + 'signature/' + this.saveQuery + '.json').subscribe(
      response =>
      {
        this.skillsNeeded = response;
        if(this.skillsNeeded == null){
          this.isNull = true;
        }
        console.log(this.skillsNeeded)
        console.log(typeof this.skillsNeeded)
      }
      );
  }

  ngOnInit() {
  }

  getPosts(){
    this.isSkill = false;
    this.isNull = false;
    this.isJob = true;
    this.saveQuery = this.searchQuery.toLowerCase();
    console.log(this.skillsNeeded)
    console.log(this.searchQuery)
    this.http.get(this.root_url + 'keywords.json/?orderBy=%22word%22&startAt="' + this.saveQuery + '"&endAt="'+ this.saveQuery + '\\uf8ff"').subscribe(
      response =>
      {
        this.relJobs = Object.values(response);
        if(Object.keys(this.relJobs).length === 0){
          this.isNull = true;
        }
        console.log(this.relJobs)
        console.log(typeof this.relJobs)
      }
      );



}



}
