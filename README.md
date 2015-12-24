# Daru::TD

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/daru/td`. To experiment with that code, run `bin/console` for an interactive prompt.

TODO: Delete this and the text above, and describe your gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'daru-td'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install daru-td

## Usage

### Create connection

```ruby
conn = Daru::TD.connect(ENV['TD_API_KEY'])
```

### Obtain a list of databases and tables as a data frame

```ruby
df_databases = conn.databases
df_tables = conn.tables('db_name')
```

### Read the query result

```ruby
engine = Daru::TD.create_engine('presto:sample_datasets', conn:conn)
df_result = Daru::TD.read_td_query(<<-SQL, engine)
  select * From nasdaq limit 3
SQL
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/daru-td.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

